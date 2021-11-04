<?php
namespace KafkaCore;

/**
 * Class KafkaProducer
 *
 * 生产者
 * @package KafkaCore
 * @author  wison
 * @since   1.0
 */
class KafkaProducer
{
	private $producer;
	// 主题
	private $topic;
	// 报告
	public $sendReport;

	/**
	 * KafkaProducer constructor.
	 * @param string $host The host of Kafka server with port like 192.168.0.192:9092
	 * @throws KafkaException
	 */
	public function __construct($host)
	{
		$host = trim($host);
		if (empty($host))
		{
			throw new KafkaException(['code' => 28, 'message' => 'host is empty']);
		}
		$conf = new \RdKafka\Conf();

		$params = [
			// 超时时间
			'socket.timeout.ms' => 10,
			'api.version.request' => 'true',
			// 消息发送失败后的重试次数
			'message.send.max.retries' => 2,
			'api.version.request.timeout.ms' => 10000,
			// 异步模式下缓冲数据的最大时间。例如设置为100则会集合100ms内的消息后发送，这样会提高吞吐量，但是会增加消息发送的延时
			'queue.buffering.max.ms' => 1,
			// 只是用于客户端启动（bootstrap）的时候有一个可以热启动的一个连接者,一旦启动完毕客户端就应该可以得知当前集群的所有节点的信息,日后集群扩展的时候客户端也能够自动实时的得到新节点的信息,即使bootstrap.servers里面的挂掉了也应该是能正常运行的,除非节点挂掉后客户端也重启了。
			'bootstrap.servers' => $host,
			// 仅获取自己用的元数据 减少带宽
			'topic.metadata.refresh.sparse' => true,
			// 设置刷新元数据时间间隔为600s 减少带宽
			'topic.metadata.refresh.interval.ms' => 600000,
			// 每隔一段时间主动关闭空闲连接，默认是10分钟。
			'log.connection.close' => 'false',
		];
		foreach ($params as $key => $value)
		{
			$conf->set($key, $value);
		}

		// 设置送达报告回调
		$conf->setDrmSgCb(function ($kafka, $message ) {
			$this->sendReport = $message->err;
			// if ( $message->err )
			// {
			// 	// 记录错误日志
			// 	logger_error('[ERROR] [KafkaProducer] kafka producer error:', [
			// 		'topic_name' => $message->topic_name,
			// 		'key'        => $message->key,
			// 		'payload'    => $message->payload,
			// 	]);
			// 	return false;
			// }
		});
		// 设置错误回调
		$conf->setErrorCb(function ($kafka, $err, $reason) {
			$errInfo = rd_kafka_err2str($err);
			$this->sendReport = $reason;
		});

		if (function_exists('pcntl_sigprocmask'))
		{
			pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
			// 设置kafka客户端线程在其完成后立即终止
			$conf->set('internal.termination.signal', SIGIO);
		}

		$this->producer = new \RdKafka\Producer($conf);
		$this->producer->addBrokers($host);
	}

	/**
	 * sendMsg
	 * @param string $topic 主题
	 * @param string $msg 消息
	 * @param null   $key 消息key,会根据key进行hash存储到对应的partition
	 * @param bool   $sync
	 * @param int    $flushTime flush time before you destory producer instance
	 * @param int    $part which partition
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function sendMsg(string $topic, string $msg, $key = null, bool $sync = false, int $flushTime = 10, int $part = 0)
	{
		if (empty($msg) || empty($topic) || $this->producer == null)
		{
			throw new KafkaException(['code' => 48, 'message' => 'topic or msg or producer is empty']);
		}
		$conf = new \RdKafka\TopicConf();
		$params = [
			/**
			 * 消息的确认模式
			 * 0：不保证消息的到达确认,只管发送,低延迟但是会出现消息的丢失
			 * 1：发送消息,并会等待leader收到确认后,具有一定的可靠性
			 * -1：发送消息,等待leader收到确认,并进行复制操作后才返回,可靠性最高
			 */
			'request.required.acks' => 1,
			// 消息发送的最长等待时间
			'request.timeout.ms' => 5000,
			'message.timeout.ms' => 5000,
		];
		foreach ($params as $k => $v)
		{
			$conf->set($k, $v);
		}
		$this->topic = $this->producer->newTopic($topic, $conf);
		$this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg, $key);
		if ( ! $sync)
		{
			$num = $this->producer->getOutQLen();
			do
			{
				$this->producer->poll(1);
			}
			while ($this->producer->getOutQLen() > 0);

			// 4.0 版开始必须调用
			for ($flushRetries = 1; $flushRetries <= $num; $flushRetries++)
			{
				$result = $this->producer->flush(1);
				if (RD_KAFKA_RESP_ERR_NO_ERROR === $result)
				{
					break;
				}
			}
		}
	}

	public function __destruct()
	{
		while ($this->producer->getOutQLen() > 0)
		{
			$this->producer->poll(1);
		}

		// 4.0 版开始必须调用
		for ($flushRetries = 0; $flushRetries < 1; $flushRetries++)
		{
			$result = $this->producer->flush(1);
			if (RD_KAFKA_RESP_ERR_NO_ERROR === $result)
			{
				break;
			}
		}
	}
}