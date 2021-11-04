<?php
namespace KafkaCore;

/**
 * Class KafkaSocketProducer
 *
 * 生产者
 * @package KafkaCore
 * @author  wison
 * @since   1.0
 */
class KafkaSocketProducer
{
	private $socket;

	/**
	 * KafkaSocketProducer constructor.
	 * @param string $host The host of Kafka server with port like 192.168.0.192:9092
	 * @param int    $port 端口
	 * @throws KafkaException
	 */
	public function __construct(string $host, int $port)
	{
		$host = trim($host);
		if (empty($host))
		{
			throw new KafkaException(['code' => 30, 'message' => 'host is empty']);
		}
		$this->socket = \socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		\socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, array("sec" => 1, "usec" => 10000));
		\socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, array("sec" => 1, "usec" => 10000));
		if (\socket_connect($this->socket, $host, $port) == false)
		{
			// 工作完毕，关闭套接流
			\socket_close($this->socket);
			throw new KafkaException(['code' => 37, 'message' => 'host connect fail']);
		}
	}

	/**
	 * sendMsg
	 * @param string $topic 主题
	 * @param string $msg 消息
	 * @return string
	 * @throws KafkaException
	 * @author wison
	 * @since  1.0
	 */
	public function sendMsg(string $topic, string $msg)
	{
		if (empty($msg) || empty($topic) || $this->socket == null)
		{
			throw new KafkaException(['code' => 55, 'message' => 'topic or msg or socket is empty']);
		}
		$data['topic'] = $topic;
		$data['msg'] = $msg;
		$message = json_encode($data);
		$message = mb_convert_encoding($message, 'GBK', 'UTF-8');
		if (\socket_write($this->socket, $message, strlen($message)) == false)
		{
			// 工作完毕，关闭套接流
			\socket_close($this->socket);
			throw new KafkaException(['code' => 63, 'message' => 'socket 写入错误']);
		}
		else
		{
			while ($callback = \socket_read($this->socket, 2048))
			{
				return $callback;
				break;
			}
		}
	}

	public function __destruct()
	{
		// 工作完毕，关闭套接流
		socket_close($this->socket);
	}
}