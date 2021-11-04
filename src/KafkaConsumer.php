<?php
namespace KafkaCore;

/**
 * Class KafkaConsumer
 *
 * 消费者
 * @package KafkaCore
 * @author  wison
 * @since   1.0
 */
class KafkaConsumer
{
	private $host;
	private $versionTimeOut;
	private static $producer;
	private static $consumer;
	private static $topicConf;

	/**
	 * KafkaConsumer constructor.
	 * @param string $host The host of Kafka server with port like 192.168.0.192:9092
	 * @param int    $versionTimeOut Max time(ms) for get api version.
	 * @throws KafkaException
	 */
	public function __construct(string $host, int $versionTimeOut)
	{
		$host = trim($host);
		if (empty($host))
		{
			throw new KafkaException(['code' => 28, 'message' => 'host is empty']);
		}
		$this->host = $host;
		$this->versionTimeOut = $versionTimeOut;
	}

	/**
	 * sendMsg
	 *
	 * @param string $topic 主题
	 * @param string $msg 消息
	 * @param int    $flushTime flush time before you destory producer instance
	 * @param int    $part which partition
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function sendMsg(string $topic, string $msg, int $flushTime = 10, int $part = 0)
	{
		if (empty($topic) || empty($msg) || self::$producer == null)
		{
			throw new KafkaException(['code' => 48, 'message' => 'topic or msg or producer is empty']);
		}

		$topic = self::$producer->newTopic($topic);
		$topic->produce(RD_KAFKA_PARTITION_UA, $part, $msg);
	    self::$producer->flush($flushTime); // 如果由于php版本原因 则注释
		self::$producer->poll(0);
	}

	/**
	 * initProducer
	 * @param int $queueBufferMaxTime
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function initProducer(int $queueBufferMaxTime)
	{
		if ( ! is_numeric($queueBufferMaxTime))
		{
			throw new KafkaException(['code' => 115, 'message' => 'queueBufferMaxTime is empty']);
		}
		$conf = new \RdKafka\Conf();
		$params = [
			'api.version.request' => 'true',
			// 消息发送失败后的重试次数
			'message.send.max.retries' => 1,
			'api.version.request.timeout.ms' => $this->versionTimeOut,
			// 异步模式下缓冲数据的最大时间。例如设置为100则会集合100ms内的消息后发送，这样会提高吞吐量，但是会增加消息发送的延时
			'queue.buffering.max.ms' => $queueBufferMaxTime,
			// 只是用于客户端启动（bootstrap）的时候有一个可以热启动的一个连接者,一旦启动完毕客户端就应该可以得知当前集群的所有节点的信息,日后集群扩展的时候客户端也能够自动实时的得到新节点的信息,即使bootstrap.servers里面的挂掉了也应该是能正常运行的,除非节点挂掉后客户端也重启了。
			'bootstrap.servers' => $this->host,
		];
		foreach ($params as $key => $value)
		{
			$conf->set($key, $value);
		}
		self::$producer = new \RdKafka\Producer($conf);
	}

	/**
	 * initConsumer
	 * @param array $topicArr 主题
	 * @param int   $groupId 分组
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function initConsumer(array $topicArr, int $groupId)
	{
		if ( ! is_numeric($groupId) || empty($topicArr))
		{
			throw new KafkaException(['code' => 138, 'message' => 'topic or groupId is empty']);
		}

		$topicParam = array(
			// 默认从最新开始
			'auto.offset.reset'       => 'largest',
			// 关闭自动提交，为保证一致性，采取手工提交
			'auto.commit.enable'      => 'false',
			// 自动提交时间间隔
			'auto.commit.interval.ms' => 1000,
			// offset存储到broker
			'offset.store.method'     => 'broker',
		);
		$topicConf = new \RdKafka\TopicConf();
		foreach ($topicParam as $key => $val)
		{
			$topicConf->set($key, $val);
		}
		self::$topicConf;

		$conf = new \RdKafka\Conf();
		$params = [
			'api.version.request' => 'true',
			'api.version.request.timeout.ms' => $this->versionTimeOut,
			// 异步模式下缓冲数据的最大时间。例如设置为100则会集合100ms内的消息后发送，这样会提高吞吐量，但是会增加消息发送的延时
			// 'queue.buffering.max.ms' => 1,
			'group.id' => $groupId,
			// 当各分区下有已提交的offset时,从提交的offset开始消费;无提交的offset时,从头开始消费
			'auto.offset.reset' => 'smallest',
			// 指定kafka节点列表,用于获取metadata,不必全部指定
			'metadata.broker.list' => $this->host,
		];
		foreach ($params as $key => $value)
		{
			$conf->set($key, $value);
		}

		self::$consumer = new \RdKafka\KafkaConsumer($conf);
		self::$consumer->subscribe($topicArr);
	}

	/**
	 * getMsg
	 * @param int $num how much msg you want get
	 * @param int $timeout read timeout
	 * @return array
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function getMsg(int $num, int $timeout = 10)
	{
		if ( ! is_numeric($num) || self::$consumer == null)
		{
			throw new KafkaException(['code' => 115, 'message' => 'num or consumer is empty']);
		}
		$msgArr = [];
		for ($i = 0; $i < $num; $i++)
		{
			// 每次消费,超时时间
			$message = self::$consumer->consume($timeout);
			switch ($message->err) {
				case RD_KAFKA_RESP_ERR_NO_ERROR:
					$msgArr[] = ['topic_name' => $message->topic_name, 'payload' => $message->payload, 'offset' => $message->offset];
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
					echo "No more messages; will wait for more\n";
					return $msgArr;
					break;
				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					echo "Timed out\n";
					return $msgArr;
					break;
				default:
					throw new \Exception($message->errstr(), $message->err);
					break;
			}
		}
		return $msgArr;
	}

	public function __destruct()
	{
		// TODO: Implement __destruct() method.
		self::$consumer = null;
		self::$producer = null;
	}
}