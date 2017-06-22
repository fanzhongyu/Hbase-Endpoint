import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.Map;

/**
 * Created by fanzhongyu on 2017/6/22.
 */
public class CoprocessorRowCounter {
    public static void main() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection hCon;
        hCon = ConnectionFactory.createConnection(conf);
        Table table = hCon.getTable(TableName.valueOf("student"));

//        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
//        Map<byte[],Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
//                null, null,
//                new Batch.Call<ExampleProtos.RowCountService,Long>() {
//                    public Long call(ExampleProtos.RowCountService counter) throws IOException {
//                        ServerRpcController controller = new ServerRpcController();
//                        BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
//                                new BlockingRpcCallback<ExampleProtos.CountResponse>();
//                        counter.getRowCount(controller, request, rpcCallback);
//                        ExampleProtos.CountResponse response = rpcCallback.get();
//                        if (controller.failedOnException()) {
//                            throw controller.getFailedOn();
//                        }
//                        return (response != null && response.hasCount()) ? response.getCount() : 0;
//                    }
//                });
    }
}
