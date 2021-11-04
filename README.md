## kafka-php-client

### composer install
`composer require wisonlau/kafkacore-client`

### composer update
`composer update wisonlau/kafkacore-client`

### Produce by Rdkafka
```php
<?php
use KafkaCore\KafkaProducer;
use KafkaCore\KafkaException;
try
{
	$producer = new KafkaProducer('192.168.0.192:9092');
	$producer->sendMsg('testtopic', 'msg');
}
catch (KafkaException $e)
{
	echo $e->getErrorMessage();
	die;
}
```
### Produce by socket
```php
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
```
### Consumer
```php
<?php

use KafkaCore\KafkaConsumer;
use KafkaCore\KafkaException;

try
{
	$consumer = new KafkaConsumer('192.168.0.192:9092', 1000);
	$consumer->initConsumer(['testtopic1', 'testtopic2'], 1);
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
```