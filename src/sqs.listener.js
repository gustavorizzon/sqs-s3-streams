class Handler {
	async main(event) {
		try {
			const [{ body, messageId }] = event.Records;
			const item = JSON.parse(body);

			console.log(
				'Event::',
				JSON.stringify({
					...item,
					messageId,
					at: new Date().toISOString()
				}, null, 2)
			)

			return {
				statusCode: 200,
				body: "Success"
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

const handler = new Handler()

module.exports = handler.main.bind(handler)
