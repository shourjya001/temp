import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DbeClientDaoImpl implements DbeClientDao {
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int LOG_INTERVAL = 50000;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private AtomicInteger totalInserted = new AtomicInteger(0);

    @Transactional
    public void saveInternalRatingEventsApi(ResponseInternalRatingsEvent internalRatingsEventResponse) {
        List<Relationships> relationshipsArrayList = internalRatingsEventResponse.getRelationships();
        int totalSize = relationshipsArrayList.size();
        System.out.println("Total records to process: " + totalSize);

        if (totalSize > 0) {
            long startTime = System.currentTimeMillis();
            
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < totalSize; i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, totalSize);
                List<Relationships> batch = relationshipsArrayList.subList(i, end);
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processBatch(batch), executorService);
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            executorService.shutdown();
            
            logProgress(totalInserted.get(), startTime);
        }
    }

    private void processBatch(List<Relationships> batch) {
        List<Object[]> batchArgs = batch.stream()
            .map(this::mapRelationshipToArgs)
            .collect(Collectors.toList());

        int inserted = executeBatch(batchArgs.toArray(new Object[0][]));
        int newTotal = totalInserted.addAndGet(inserted);
        
        if (newTotal % LOG_INTERVAL == 0) {
            logProgress(newTotal, System.currentTimeMillis());
        }
    }

    private Object[] mapRelationshipToArgs(Relationships relationship) {
        return new Object[] {
            relationship.getBdrId(),
            relationship.getBusinessEntity(),
            relationship.getNature(),
            relationship.getStatus(),
            null, // Skipping reason.getGoldenBdrId()
            null  // Skipping reason.getLabel()
        };
    }

    private int executeBatch(Object[][] batchArgs) {
        int[] updateCounts = jdbcTemplate.batchUpdate(QRY_SAVE_INTERNALRATINGSEVENTS.value(), batchArgs);
        return Arrays.stream(updateCounts).sum();
    }

    private void logProgress(int totalInserted, long startTime) {
        long currentTime = System.currentTimeMillis();
        long elapsedSeconds = (currentTime - startTime) / 1000;
        double recordsPerSecond = totalInserted / (double) Math.max(1, elapsedSeconds);
        System.out.println("Inserted " + totalInserted + " records. Rate: " + String.format("%.2f", recordsPerSecond) + " records/second");
    }
}
