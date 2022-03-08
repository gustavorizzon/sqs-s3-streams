const AWS = require('aws-sdk')
const { Writable, pipeline } = require('stream')
const csvtojson = require('csvtojson')

class Handler {
	constructor({ s3Svc, sqsSvc }) {
		this.s3Svc = s3Svc;
		this.sqsSvc = sqsSvc;
		this.queueName = process.env.SQS_QUEUE;
	}

	static getSdks() {
		const host = process.env.LOCALSTACK_HOST || 'localhost';
		const port = process.env.EDGE_PORT || '4566';
		const isLocal = process.env.IS_LOCAL;
		
		const s3Config = {
			endpoint: new AWS.Endpoint(`http://${host}:${port}`),
			s3ForcePathStyle: true
		};

		const sqsConfig = {
			endpoint: new AWS.Endpoint(`http://${host}:${port}`),
			s3ForcePathStyle: true
		};

		if (!isLocal) {
			delete s3Config.endpoint;
			delete sqsConfig.endpoint;
		}

		return {
			s3: new AWS.S3(s3Config),
			sqs: new AWS.SQS(sqsConfig)
		}
	}

	async getQueueUrl() {
		console.log('getQueueUrl > ', this.queueName)
		const { QueueUrl } = await this.sqsSvc.getQueueUrl({
			QueueName: this.queueName
		}).promise()

		return QueueUrl;
	}

	processDataOnDemand(queueUrl) {
		const writableStream = new Writable({
			write: (chunk, encoding, done) => {
				const item = chunk.toString();
				console.log('sending...', item, 'at', new Date().toISOString());
				this.sqsSvc.sendMessage({
					QueueUrl: queueUrl,
					MessageBody: item
				}, done)
			}
		})

		return writableStream;
	}

	async pipefyStream(...args) {
		return new Promise((resolve, reject) => {
			pipeline(...args, error => error ? reject(error) : resolve())
		})
	}

	async main(event) {
		console.log('Event >', JSON.stringify(event, null, 2))

		const [
			{
				s3: {
					bucket: { name },
					object: { key }
				}
			}
		] = event.Records;

		console.log('Processing > bucket > ', name,'> file >', key)
		try {
			const queueUrl = await this.getQueueUrl();

			console.log('QueueUrl > ', queueUrl)

			const params = {
				Bucket: name, Key: key
			};

			await this.pipefyStream(
				this.s3Svc.getObject(params)
					.createReadStream(),
					csvtojson(),
					this.processDataOnDemand(queueUrl)
			)

			console.log('process finished > ', new Date().toISOString())

			return {
				statusCode: 200,
				body: "Process finished with success"
			}
		} catch (error) {
			console.log("Error::", error.trace)

			return {
				statusCode: 500,
				body: 'Internal server error'
			}
		}
	}
}

const { s3, sqs } = Handler.getSdks();
const handler = new Handler({
	s3Svc: s3,
	sqsSvc: sqs
})


module.exports = handler.main.bind(handler)
