package tw.gasol.exam.concurrency;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.AddInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddInstanceGroupsResult;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.SetTerminationProtectionRequest;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;

/**
 * @author Gasol Wu <gasol.wu@gmail.com>
 */
public class MockAmazonElasticMapReduceClient implements AmazonElasticMapReduce {

	private StringBuilder sb = new StringBuilder();
	
	@Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
		append("setEndpoint");
	}

	@Override
	public AddInstanceGroupsResult addInstanceGroups(
			AddInstanceGroupsRequest addInstanceGroupsRequest)
			throws AmazonServiceException, AmazonClientException {
		append("addInstanceGroups");
		return null;
	}

	@Override
	public void addJobFlowSteps(AddJobFlowStepsRequest addJobFlowStepsRequest)
			throws AmazonServiceException, AmazonClientException {
		append("addJobFlowSteps");
	}

	@Override
	public void terminateJobFlows(
			TerminateJobFlowsRequest terminateJobFlowsRequest)
			throws AmazonServiceException, AmazonClientException {
		append("terminateJobFlows");
	}

	@Override
	public DescribeJobFlowsResult describeJobFlows(
			DescribeJobFlowsRequest describeJobFlowsRequest)
			throws AmazonServiceException, AmazonClientException {
		append("describeJobFlows");
		return null;
	}

	@Override
	public void setTerminationProtection(
			SetTerminationProtectionRequest setTerminationProtectionRequest)
			throws AmazonServiceException, AmazonClientException {
		append("setTerminationProtection");
	}

	@Override
	public RunJobFlowResult runJobFlow(RunJobFlowRequest runJobFlowRequest)
			throws AmazonServiceException, AmazonClientException {
		append("runJobFlow");
		return null;
	}
	
	@Override
	public void modifyInstanceGroups(
			ModifyInstanceGroupsRequest modifyInstanceGroupsRequest)
			throws AmazonServiceException, AmazonClientException {
		append("modifyInstanceGroups");
	}

	@Override
	public DescribeJobFlowsResult describeJobFlows()
			throws AmazonServiceException, AmazonClientException {
		append("describeJobFlows");
		return null;
	}

	@Override
	public void modifyInstanceGroups() throws AmazonServiceException,
			AmazonClientException {
		append("modifyInstanceGroups");
	}

	@Override
	public void shutdown() {
		append("shutdown");
	}
	
	@Override
	public ResponseMetadata getCachedResponseMetadata(
			AmazonWebServiceRequest request) {
		append("getCachedResponseMetadata");
		return null;
	}
	
	private void append(String str) {
		Thread thread = Thread.currentThread();
		System.out.println("[" + thread.getName() + "] method: " + str + " => " + System.currentTimeMillis());
		sb.append(str);
	}
	
	@Override
	public String toString() {
		return sb.toString();
	}
}
