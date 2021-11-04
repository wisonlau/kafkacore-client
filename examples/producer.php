<?php

use KafkaCore\KafkaProducer;
use KafkaCore\KafkaException;

try
{
	$producer = new KafkaProducer('192.168.0.192:9092');
	$producer->sendMsg('testtopic1', 'msg');
}
catch (KafkaException $e)
{
	echo $e->getErrorMessage();
	die;
}