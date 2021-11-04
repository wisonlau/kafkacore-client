<?php

use KafkaCore\KafkaConsumer;
use KafkaCore\KafkaException;

try
{
	$consumer = new KafkaConsumer('192.168.0.192:9092', 1000);
	$consumer->initConsumer(['testtopic1', 'testtopic2'], 0);
	while(true)
	{
		$msg = $consumer->getMsg(1, 1000);
		// todo more
	}

}
catch (KafkaException $e)
{
	echo $e->getErrorMessage();
	die;
}