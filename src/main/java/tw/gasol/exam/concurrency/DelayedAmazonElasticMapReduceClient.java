package tw.gasol.exam.concurrency;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public class DelayedAmazonElasticMapReduceClient implements AmazonElasticMapReduce {

	private AmazonElasticMapReduce delegate;
	
	private ExecutorService executor;
	
	public DelayedAmazonElasticMapReduceClient(AmazonElasticMapReduce client) {
		this(client, 5);
	}
	
	public DelayedAmazonElasticMapReduceClient(AmazonElasticMapReduce client, int delay) {
		delegate = client;
		executor = new MySingleThreadExecutor(delay);
	}
	
	@Override
	public void setEndpoint(String endpoint) throws IllegalArgumentException {
		delegate.setEndpoint(endpoint);
	}

	@Override
	public AddInstanceGroupsResult addInstanceGroups(
			AddInstanceGroupsRequest addInstanceGroupsRequest)
			throws AmazonServiceException, AmazonClientException {
		final AddInstanceGroupsRequest request = addInstanceGroupsRequest;
		Callable<AddInstanceGroupsResult> task = new Callable<AddInstanceGroupsResult>() {
			@Override
			public AddInstanceGroupsResult call() throws Exception {
				return delegate.addInstanceGroups(request);
			}
		};
		try {
			return executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public void addJobFlowSteps(AddJobFlowStepsRequest addJobFlowStepsRequest)
			throws AmazonServiceException, AmazonClientException {
		
		final AddJobFlowStepsRequest request = addJobFlowStepsRequest;
		
		Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				delegate.addJobFlowSteps(request);
				return null;
			}
		};
		
		try {
			executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public void terminateJobFlows(
			TerminateJobFlowsRequest terminateJobFlowsRequest)
			throws AmazonServiceException, AmazonClientException {
		final TerminateJobFlowsRequest request = terminateJobFlowsRequest;
		
		Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				delegate.terminateJobFlows(request);
				return null;
			}
		};
		
		try {
			executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public DescribeJobFlowsResult describeJobFlows(
			DescribeJobFlowsRequest DescribeJobFlowsRequest)
			throws AmazonServiceException, AmazonClientException {
		final DescribeJobFlowsRequest request = DescribeJobFlowsRequest;
		
		Callable<DescribeJobFlowsResult> task = new Callable<DescribeJobFlowsResult>() {
			@Override
			public DescribeJobFlowsResult call() throws Exception {
				return delegate.describeJobFlows(request);
			}
		};
		
		try {
			return executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}
	
	@Override
	public void setTerminationProtection(
			SetTerminationProtectionRequest setTerminationProtectionRequest)
			throws AmazonServiceException, AmazonClientException {
		final SetTerminationProtectionRequest request = setTerminationProtectionRequest;
		
		Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				delegate.setTerminationProtection(request);
				return null;
			}
		};
		
		try {
			executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public RunJobFlowResult runJobFlow(RunJobFlowRequest runJobFlowRequest)
			throws AmazonServiceException, AmazonClientException {
		final RunJobFlowRequest request = runJobFlowRequest;
		
		Callable<RunJobFlowResult> task = new Callable<RunJobFlowResult>() {
			@Override
			public RunJobFlowResult call() throws Exception {
				return delegate.runJobFlow(request);
			}
		};
		
		try {
			return executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public void modifyInstanceGroups(
			ModifyInstanceGroupsRequest modifyInstanceGroupsRequest)
			throws AmazonServiceException, AmazonClientException {
		final ModifyInstanceGroupsRequest request = modifyInstanceGroupsRequest;
		
		Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				delegate.modifyInstanceGroups(request);
				return null;
			}
		};
		
		try {
			executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public DescribeJobFlowsResult describeJobFlows()
			throws AmazonServiceException, AmazonClientException {
		Callable<DescribeJobFlowsResult> task = new Callable<DescribeJobFlowsResult>() {
			@Override
			public DescribeJobFlowsResult call() throws Exception {
				return delegate.describeJobFlows();
			}
		};
		
		try {
			Future<DescribeJobFlowsResult> future = executor.submit(task);
			return future.get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public void modifyInstanceGroups() throws AmazonServiceException,
			AmazonClientException {
		Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				delegate.modifyInstanceGroups();
				return null;
			}
		};
		
		try {
			Future<Void> future = executor.submit(task);
			future.get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	@Override
	public void shutdown() {
		delegate.shutdown();
	}

	@Override
	public ResponseMetadata getCachedResponseMetadata(
			AmazonWebServiceRequest amazonWebServiceRequest) {
		final AmazonWebServiceRequest request = amazonWebServiceRequest;
		Callable<ResponseMetadata> task = new Callable<ResponseMetadata>() {
			@Override
			public ResponseMetadata call() throws Exception {
				return delegate.getCachedResponseMetadata(request);
			}
		};
		
		try {
			return executor.submit(task).get();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}
	}

	class MySingleThreadExecutor extends ThreadPoolExecutor {
		
		private long lastTimeExecuted = 0;
		private int delay = 0;
		
		public MySingleThreadExecutor(int delay) {
			super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
			this.delay = delay;
		}
		
		@Override
		protected void beforeExecute(Thread t, Runnable r) {
			super.beforeExecute(t, r);
			
			long idleTime = System.currentTimeMillis() - lastTimeExecuted;
			int delayedInMillis = delay * 1000;
			if (idleTime > delayedInMillis) {
				return;
			}
			try {
				Thread.sleep(delayedInMillis - idleTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			super.afterExecute(r, t);
			
			lastTimeExecuted = System.currentTimeMillis();
		}		
		
		public int getDelayed() {
			return delay;
		}
		
	}
	
	public void stop() {
		executor.shutdown();
	}
	
	public static void main(String[] args) {
		int delay = 0;
		try {
			delay = Integer.parseInt(args[0]);
		} catch (Exception e) {
			System.err.println("The first argument must be an integer.");
			System.err.print("Caused: ");
			e.printStackTrace();
			System.exit(1);
		}
		MockAmazonElasticMapReduceClient client = new MockAmazonElasticMapReduceClient();
		final DelayedAmazonElasticMapReduceClient wrapper = new DelayedAmazonElasticMapReduceClient(client, delay);
		
		StringBuilder sb = new StringBuilder();
		
		Method[] methods = AmazonElasticMapReduce.class.getMethods();
		List<Method> methodList = Arrays.asList(methods);
		Collections.shuffle(methodList);
		
		for (Iterator<Method> iterator = methodList.iterator(); iterator.hasNext();) {
			final Method method = iterator.next();
			
			sb.append(method.getName());
			
			Class<?>[] parameterTypes = method.getParameterTypes();
			final Object[] methodArgs = new Object[parameterTypes.length];
			try {
				Thread t = new Thread() {
					@Override
					public void run() {
						try {
							method.invoke(wrapper, methodArgs);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				t.start();
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		wrapper.stop();
		assert client.toString().equals(sb.toString());
	}
}
