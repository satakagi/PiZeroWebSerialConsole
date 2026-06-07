export class SerialManager {
	constructor() {
		this.port = null;
		this.reader = null;
		this.writer = null;
		this.isConnecting = false;
		this.receiveBuffer = "";
		this.currentWaiter = null;
		this.onDataReceived = null; // ターミナル描画用のフック

		// --- 追加: キュー管理用の変数 ---
		this.queue = [];             // 実行待ちのコマンドを格納するキュー
		this.isProcessingQueue = false; // 現在キューを消化中かどうかのフラグ
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

	// --- 変更: 外部から呼ばれるメソッド（直接実行せずキューに積む） ---
	async writeAndWaitFor(data, expectedRegExp, timeoutMs = 30000) {
		return new Promise((resolve, reject) => {
			// タスクをキューの末尾に追加
			this.queue.push({ data, expectedRegExp, timeoutMs, resolve, reject });
			// キューの消化を開始（既に実行中ならそのまま無視される）
			this.processQueue();
		});
	}

	// --- 追加: キューを順番に消化する内部メソッド ---
	async processQueue() {
		// 既に処理中、またはキューが空なら何もしない
		if (this.isProcessingQueue || this.queue.length === 0) return;
		
		this.isProcessingQueue = true;
		
		// キューの先頭からタスクを取り出す（FIFO）
		const task = this.queue.shift();

		try {
			// 実際の通信処理を実行（プロンプトが返るまでここで待機される）
			const result = await this._executeCommand(task.data, task.expectedRegExp, task.timeoutMs);
			task.resolve(result); // 呼び出し元（awaitしている箇所）へ結果を返す
		} catch (error) {
			task.reject(error);   // タイムアウトなどのエラーを返す
		} finally {
			this.isProcessingQueue = false;
			// 次のタスクがあれば再帰的に処理を開始
			this.processQueue();
		}
	}

	// --- 変更: 実際の通信待機処理（旧 writeAndWaitFor の中身） ---
	_executeCommand(data, expectedRegExp, timeoutMs) {
		// 次のコマンド出力と混ざらないよう、実行直前にバッファをクリア
		this.receiveBuffer = "";

		return new Promise(async (resolve, reject) => {
			const timeoutId = setTimeout(() => {
				this.currentWaiter = null;
				reject(new Error(`Timeout waiting for: ${expectedRegExp}`));
			}, timeoutMs);

			// checkWaiter() で判定するための設定を保持
			this.currentWaiter = {
				regex:
					typeof expectedRegExp === "string"
						? new RegExp(expectedRegExp)
						: expectedRegExp,
				resolve, // ここが呼ばれると _executeCommand の Promise が解決される
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
							this.checkWaiter(); // 受信のたびに待機条件を満たしたかチェック
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
			this.receiveBuffer = "";     // 次の待機のためにクリア
			this.currentWaiter = null;   // 待機状態を解除
			resolve(result);             // _executeCommand の待機を解除
		}
	}

	removeControlChars(str) {
		return str.replace(
			/[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g,
			""
		);
	}
}
