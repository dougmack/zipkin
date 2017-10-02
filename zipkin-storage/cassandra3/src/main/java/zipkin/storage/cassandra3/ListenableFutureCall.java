package zipkin.storage.cassandra3;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import javax.annotation.Nullable;
import zipkin2.Call;
import zipkin2.Callback;

abstract class ListenableFutureCall<V> extends Call<V> {
  volatile boolean canceled;
  boolean executed;
  volatile ListenableFuture<V> future;

  protected ListenableFutureCall() {
  }

  @Override public final V execute() throws IOException {
    synchronized (this) {
      if (executed) throw new IllegalStateException("Already Executed");
      executed = true;
    }

    if (isCanceled()) throw new IOException("Canceled");
    return Futures.getUnchecked(future = newFuture());
  }

  @Override public final void enqueue(Callback<V> callback) {
    synchronized (this) {
      if (executed) throw new IllegalStateException("Already Executed");
      executed = true;
    }

    if (isCanceled()) {
      callback.onError(new IOException("Canceled"));
    } else {
      Futures.addCallback((future = newFuture()), new FutureCallback<V>() {
        @Override public void onSuccess(@Nullable V result) {
          callback.onSuccess(result);
        }

        @Override public void onFailure(Throwable t) {
          callback.onError(t);
        }
      });
    }
  }

  protected abstract ListenableFuture<V> newFuture();

  @Override public final void cancel() {
    canceled = true;
    ListenableFuture<V> maybeFuture = future;
    if (maybeFuture != null) maybeFuture.cancel(true);
  }

  @Override public final boolean isCanceled() {
    if (canceled) return true;
    ListenableFuture<V> maybeFuture = future;
    return maybeFuture != null && maybeFuture.isCancelled();
  }
  @Override public Call<V> clone() {
    throw new UnsupportedOperationException("one-shot deal");
  }
}
