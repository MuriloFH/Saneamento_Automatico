import datetime


class LogTxt:
    def __init__(self):
        self.arquivoTxt = open(f"logExecucao{datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt", mode="w", encoding="utf-8")

    def escreveLog(self, conteudo):
        try:
            self.arquivoTxt.write(f"{conteudo}\n")
        except Exception as e:
            print(e)

    def fechaArquivo(self):
        self.arquivoTxt.close()
