const topic = "bddcy39c-default" // set the correct topic name, especially when you are using CloudKarafka

const kafkaConfig = {
    "group.id": "cloudkarafka-example",
    "metadata.broker.list": "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094".split(","),
    "socket.keepalive.enable": true,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-256",
    "sasl.username": "bddcy39c",
    "sasl.password": "e8hPouz3LL2rhp_vtQhp547rYsr9BbhQ",
    "debug": "generic,broker,security"
};

module.exports = {kafkaConfig, topic};
