QUEUE_URL=$1

echo 'Sending message...' $QUEUE_URL

aws \
	sqs send-message \
	--queue-url $QUEUE_URL \
	--message-body 'Hey ho lets go' \
	--endpoint-url=http://localhost:4566


# comentar o comando abaixo se esse script for pra prod
aws \
	sqs receive-message \
	--queue-url $QUEUE_URL \
	--endpoint-url=http://localhost:4566
