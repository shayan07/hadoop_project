/**
 * Created by shayan on 7/2/17.
 */

public class Driver {
    public static void main(String[] args) throws Exception {
        int k = 100;
        DedupByPid dedupByPid = new DedupByPid();
        TopK topK = new TopK();
        String localPath = "/Users/shayan/workstation/HadoopProject";

        String rawInput = localPath + "/input/";
        String dedupByPidOutPut = localPath + "/output/dedupByPidOutPut/";
        String topKInput = dedupByPidOutPut;
        String topKOutput = localPath + "/output/topK/";

        String[] path1 = {rawInput, dedupByPidOutPut};
        String[] path2 = {topKInput, topKOutput, Integer.toString(k)};


        dedupByPid.main(path1);
        topK.main(path2);

    }
}

