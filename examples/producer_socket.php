<?php

use KafkaCore\KafkaSocketProducer;
use KafkaCore\KafkaException;

try
{
	$producer = new KafkaSocketProducer('192.168.0.192', '9099');
	$response = $producer->sendMsg('testtopic', 'msg');
}
catch (KafkaException $e)
{
	echo $e->getErrorMessage();
}