export class SerialManager {
	constructor() {
		this.port = null;
		this.reader = null;
		this.writer = null;
		this.isConnecting = false;
		this.receiveBuffer = "";
		this.currentWaiter = null;
		this.onDataReceived = null; // ターミナル描画用のフック
	}

	async connect(baudRate = 115200) {
		this.port = await navigator.serial.requestPort();
		await this.port.open({ baudRate });
		this.isConnecting = true;
		this.startReadLoop();
	}

	async disconnect() {
		this.isConnecting = false;
		await this.write(" ");
		await new Promise((resolve) => setTimeout(resolve, 10)); // tiny sleep
		if (this.port) await this.port.close();
	}

	async write(data) {
		if (!this.port) return;
		const encoder = new TextEncoder();
		this.writer = this.port.writable.getWriter();
		await this.writer.write(encoder.encode(data));
		this.writer.releaseLock();
	}

	async writeAndWaitFor(data, expectedRegExp, timeoutMs = 30000) {
		if (this.currentWaiter) {
			console.warn(
				"Already waiting for a response, overriding might cause issues."
			);
		}

		this.receiveBuffer = "";

		return new Promise(async (resolve, reject) => {
			const timeoutId = setTimeout(() => {
				this.currentWaiter = null;
				reject(new Error(`Timeout waiting for: ${expectedRegExp}`));
			}, timeoutMs);

			this.currentWaiter = {
				regex:
					typeof expectedRegExp === "string"
						? new RegExp(expectedRegExp)
						: expectedRegExp,
				resolve,
				timeoutId,
			};

			await this.write(data);
		});
	}

	async startReadLoop() {
		try {
			while (this.port.readable && this.isConnecting) {
				this.reader = this.port.readable.getReader();
				try {
					while (this.isConnecting) {
						const { value, done } = await this.reader.read();
						if (done) break;

						if (value) {
							const decoded = new TextDecoder("utf-8").decode(value);

							// ターミナルへ即時描画
							if (this.onDataReceived) this.onDataReceived(decoded);

							this.receiveBuffer += decoded;
							this.checkWaiter();
						}
					}
				} finally {
					this.reader.releaseLock();
				}
			}
		} catch (error) {
			console.error("Read loop error:", error);
		}
	}

	checkWaiter() {
		if (!this.currentWaiter) return;

		const { regex, resolve, timeoutId } = this.currentWaiter;
		if (this.receiveBuffer.match(regex)) {
			clearTimeout(timeoutId);
			const result = this.removeControlChars(this.receiveBuffer);
			this.receiveBuffer = "";
			this.currentWaiter = null;
			resolve(result);
		}
	}

	removeControlChars(str) {
		return str.replace(
			/[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g,
			""
		);
	}
}
