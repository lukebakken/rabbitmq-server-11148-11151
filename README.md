# Reproduction for rabbitmq/rabbitmq-server#11148

https://github.com/rabbitmq/rabbitmq-server/discussions/11148

## Start RabbitMQ

```
./run-rabbitmq.sh
```

## Run Python QPID sender

```
cd py
pipenv install
pipenv shell
PN_TRACE_FRM=1 python sender.py --address amqp://localhost:5672/exampleQueue -m 1
```
