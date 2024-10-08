import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public class DbeClientDaoImpl implements DbeClientDao {
    private static final int BATCH_SIZE = 1000;
    private static final int LOG_INTERVAL = 10000;

    @PersistenceContext
    private EntityManager entityManager;

    private AtomicInteger totalInserted = new AtomicInteger(0);

    @Transactional
    public void saveInternalRatingEventsApi(ResponseInternalRatingsEvent internalRatingsEventResponse) {
        List<Relationships> relationshipsArrayList = internalRatingsEventResponse.getRelationships();
        int totalSize = relationshipsArrayList.size();
        System.out.println("Total records to process: " + totalSize);

        if (totalSize > 0) {
            long startTime = System.currentTimeMillis();
            processBatch(relationshipsArrayList);
            logProgress(totalInserted.get(), startTime);
        }
    }

    private void processBatch(List<Relationships> relationships) {
        for (int i = 0; i < relationships.size(); i++) {
            Relationships relationship = relationships.get(i);
            List<Reasons> reasonsArrayList = relationship.getReasons();

            InternalRatingEvent event = new InternalRatingEvent();
            event.setBdrId(relationship.getBdrId());
            event.setBusinessEntity(relationship.getBusinessEntity());
            event.setNature(relationship.getNature());
            event.setStatus(relationship.getStatus());
            if (!reasonsArrayList.isEmpty()) {
                // Check if these methods exist in your Reasons class
                if (reasonsArrayList.get(0).getGoldenBdrId() != null) {
                    event.setGoldenBdrId(reasonsArrayList.get(0).getGoldenBdrId());
                }
                if (reasonsArrayList.get(0).getLabel() != null) {
                    event.setLabel(reasonsArrayList.get(0).getLabel());
                }
            }

            entityManager.merge(event);

            if (i % BATCH_SIZE == 0 || i == relationships.size() - 1) {
                entityManager.flush();
                entityManager.clear();
                int newTotal = totalInserted.addAndGet(i % BATCH_SIZE == 0 ? BATCH_SIZE : i % BATCH_SIZE + 1);
                if (newTotal % LOG_INTERVAL == 0) {
                    logProgress(newTotal, System.currentTimeMillis());
                }
            }
        }
    }

    private void logProgress(int totalInserted, long startTime) {
        long currentTime = System.currentTimeMillis();
        long elapsedSeconds = (currentTime - startTime) / 1000;
        double recordsPerSecond = totalInserted / (double) Math.max(1, elapsedSeconds);
        System.out.println("Inserted " + totalInserted + " records. Rate: " + String.format("%.2f", recordsPerSecond) + " records/second");
    }
}
