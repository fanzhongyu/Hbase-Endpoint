import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.Map;

/**
 * Created by fanzhongyu on 2017/6/22.
 */
public class CoprocessorRowCounter {
    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir","hdfs://valukacluster/hbase");
        conf.set("hbase.zookeeper.quorum","zk-1,zk-2,zk-3");
        conf.set("hbase.zookeeper.property.clientPort","30000");
        Connection hCon;
        hCon = ConnectionFactory.createConnection(conf);
        Table table = hCon.getTable(TableName.valueOf("student"));

        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
        Map<byte[],Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
                null, null,
                new Batch.Call<ExampleProtos.RowCountService,Long>() {
                    public Long call(ExampleProtos.RowCountService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
                                new BlockingRpcCallback<ExampleProtos.CountResponse>();
                        counter.getRowCount(controller, request, rpcCallback);
                        ExampleProtos.CountResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasCount()) ? response.getCount() : 0;
                    }
                });
        long sum = 0;
        int count = 0;
        for(Long l : results.values()){
            sum += l;
            count ++;
        }

        System.out.println("Region count " + count);
        System.out.println("Rowkey count " + sum);

    }
}
