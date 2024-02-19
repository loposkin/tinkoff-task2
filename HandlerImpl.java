import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class HandlerImpl implements Handler {
    private final Client client;

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public Duration timeout() {
        return Duration.ZERO;
    }

    @Override
    public void performOperation() {
        Event event = this.client.readData();
        Payload payload = event.payload();
        try {
            CompletableFuture.allOf(event.recipients().stream()
                    .map(address -> CompletableFuture.runAsync(() -> sendPayload(address, payload)))
                    .toArray(CompletableFuture[]::new))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    private void sendPayload(Address dest, Payload payload) {
        while (true) {
            Result result = client.sendData(dest, payload);
            if (result.equals(Result.ACCEPTED)) {
                return;
            } else {
                try {
                    wait(timeout().toMillis());
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
