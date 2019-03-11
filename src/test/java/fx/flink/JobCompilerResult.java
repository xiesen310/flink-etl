package fx.flink;

import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.List;

/**
 * @author zhangdekun on 2019/2/14.
 */
public class JobCompilerResult {
    private JobGraph jobGraph;
    private List<String> additionalJars;

    public List<String> getAdditionalJars() {
        return additionalJars;
    }

    public void setAdditionalJars(List<String> additionalJars) {
        this.additionalJars = additionalJars;
    }


    public JobGraph getJobGraph() {
        return jobGraph;
    }

    public void setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
    }
}
