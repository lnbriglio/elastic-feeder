const config = require("./config");
const faker = require("faker");
const { Client } = require("@elastic/elasticsearch");
const client = new Client({ node: config.elastic.url });
const cluster = require("cluster");
// const numCPUs = require('os').cpus().length;
const numCPUs = 10;

const sampleSize = 100;
const bulkSize = 1000;

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
  });
} else {
  // Workers can share any TCP connection
  // In this case it is an HTTP server
  run().catch(console.log);

  console.log(`Worker ${process.pid} started`);
}

async function run() {
  for (let i = 0; i < sampleSize; i++) {
    const body = buildBulkRequestBody();
    await client.bulk({ body });
  }
}

// run().catch(console.log);

function buildBulkRequestBody() {
  const dataset = [];
  for (let i = 0; i < bulkSize; i++) {
    dataset.push(buildSampleItem());
  }

  return dataset.flatMap(doc => [
    { index: { _index: config.elastic.indexName } },
    doc
  ]);
}

function buildSampleItem() {
  return {
    name: faker.name.firstName(),
    lastName: faker.name.lastName(),
    address: {
      street: faker.address.streetAddress(),
      city: faker.address.city(),
      state: faker.address.state()
    },
    birthDate: faker.date.past(25),
    work: {
      name: faker.company.companyName(),
      url: faker.internet.url(),
      address: {
        street: faker.address.streetAddress(),
        city: faker.address.city(),
        state: faker.address.state()
      },
      salary: faker.random.number(10000, 100000)
    }
  };
}
