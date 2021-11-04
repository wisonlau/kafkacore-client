<?php
namespace KafkaCore;

class KafkaException extends \Exception
{
	private $details = array();

	function __construct($details)
	{
		if (is_array($details))
		{
			$message = $details['code'] . ': ' . $details['message'];
			parent::__construct($message);
			$this->details = $details;
		}
		else
		{
			$message = $details;
			parent::__construct($message);
		}
	}

	/**
	 * getErrorCode
	 * @return mixed|string
	 * @author wison
	 * @since  1.0
	 */
	public function getErrorCode()
	{
		return isset($this->details['code']) ? $this->details['code'] : '';
	}

	/**
	 * getErrorMessage
	 * @return mixed|string
	 * @author wison
	 * @since  1.0
	 */
	public function getErrorMessage()
	{
		return isset($this->details['message']) ? $this->details['message'] : '';
	}

}