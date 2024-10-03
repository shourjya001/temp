import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

public class DbeClientDaoImpl implements DbeClientDao {

    // Define constants for batch size, when to split work, and how often to log
    private static final int BATCH_SIZE = 5000;
    private static final int PARALLELISM_THRESHOLD = 10000;
    private static final int LOG_INTERVAL = 10000;

    // Tool for database operations
    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Counter for total inserted records, safe for multiple threads
    private AtomicInteger totalInserted = new AtomicInteger(0);

    // Main method to start the insertion process
    @Transactional
    public void saveInternalRatingEventsApi(ResponseInternalRatingsEvent internalRatingsEventResponse) {
        // Get the list of relationships to insert
        List<Relationships> relationshipsArrayList = internalRatingsEventResponse.getRelationships();
        int totalSize = relationshipsArrayList.size();
        System.out.println("Total records to process: " + totalSize);

        if (totalSize > 0) {
            // Record the start time
            long startTime = System.currentTimeMillis();
            
            // Create a pool of worker threads
            ForkJoinPool pool = ForkJoinPool.commonPool();
            // Create a task to insert all records
            InsertTask task = new InsertTask(relationshipsArrayList, 0, totalSize);
            // Start the task
            pool.invoke(task);

            // Log the final progress
            logProgress(totalInserted.get(), startTime);
        }
    }

    // A task that can split itself into smaller tasks
    private class InsertTask extends RecursiveAction {
        private final List<Relationships> relationships;
        private final int start;
        private final int end;

        // Constructor to define which part of the list this task should work on
        InsertTask(List<Relationships> relationships, int start, int end) {
            this.relationships = relationships;
            this.start = start;
            this.end = end;
        }

        // Method that decides whether to do the work or split it
        @Override
        protected void compute() {
            if (end - start <= PARALLELISM_THRESHOLD) {
                // If the task is small enough, do the work
                processBatch();
            } else {
                // Otherwise, split the task into two
                int mid = start + (end - start) / 2;
                invokeAll(new InsertTask(relationships, start, mid),
                          new InsertTask(relationships, mid, end));
            }
        }

        // Method to process and insert a batch of records
        private void processBatch() {
            // Prepare a batch of records to insert
            Object[][] batchArgs = new Object[BATCH_SIZE][6];
            int batchIndex = 0;

            for (int i = start; i < end; i++) {
                // Get the current relationship
                Relationships relationship = relationships.get(i);
                List<Reasons> reasonsArrayList = relationship.getReasons();

                // Prepare the data for this relationship
                batchArgs[batchIndex] = new Object[] {
                    relationship.getBdrId(),
                    relationship.getBusinessEntity(),
                    relationship.getNature(),
                    relationship.getStatus(),
                    reasonsArrayList.isEmpty() ? null : reasonsArrayList.get(0).getGoldenBdrId(),
                    reasonsArrayList.isEmpty() ? null : reasonsArrayList.get(0).getLabel()
                };

                batchIndex++;

                // If we've filled a batch or reached the end, insert the records
                if (batchIndex == BATCH_SIZE || i == end - 1) {
                    int inserted = executeBatch(batchIndex == BATCH_SIZE ? batchArgs : Arrays.copyOf(batchArgs, batchIndex));
                    int newTotal = totalInserted.addAndGet(inserted);
                    // Log progress at regular intervals
                    if (newTotal % LOG_INTERVAL == 0) {
                        logProgress(newTotal, System.currentTimeMillis());
                    }
                    batchIndex = 0;
                }
            }
        }
    }

    // Method to actually insert records into the database
    private int executeBatch(Object[][] batchArgs) {
        int[] updateCounts = jdbcTemplate.batchUpdate(QRY_SAVE_INTERNALRATINGSEVENTS.value(), Arrays.asList(batchArgs));
        return Arrays.stream(updateCounts).sum();
    }

    // Method to log the progress of insertions
    private void logProgress(int totalInserted, long startTime) {
        long currentTime = System.currentTimeMillis();
        long elapsedSeconds = (currentTime - startTime) / 1000;
        double recordsPerSecond = totalInserted / (double) Math.max(1, elapsedSeconds);
        System.out.println("Inserted " + totalInserted + " records. Rate: " + String.format("%.2f", recordsPerSecond) + " records/second");
    }
}
