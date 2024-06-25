import sys
import psycopg2


class LogAlteracoes:

    def __init__(self, host, dataBase, usuario, senha):
        self.host = host
        self.dataBase = dataBase
        self.usuario = usuario
        self.senha = senha

    def conectaBanco(self):

        try:
            conexao = psycopg2.connect(host=self.host,
                                       database=self.dataBase,
                                       user=self.usuario,
                                       password=self.senha)
            return conexao
        except Exception as ex:
            print(ex)
            sys.exit()

    def criaTabela(self):
        banco = self.conectaBanco()

        try:
            banco.cursor().execute(f"""
                create table if not exists logAlteracoes (
                  nomeEntidade VARCHAR(100),
                  sistema VARCHAR(50),
                  tipoValidacao VARCHAR(50),
                  preValidacao VARCHAR(100),
                  dadoAlterado VARCHAR(500),
                  dataAlteracao TIMESTAMP,
                  tipoAlteracao VARCHAR(50)
                );
            """)

            banco.commit()
            banco.close()
        except Exception as exept:
            print(f"Erro ao criar a tabela. {exept}")

    def insereLog(self, entidade, sistema, tipoValidacao, preValidacao, dadoAlterado, dataAlteracao, tipoAlteracao):
        banco = self.conectaBanco()
        comando = ""
        if len(dadoAlterado) > 0:
            for alteracao in dadoAlterado:
                comando += f"insert into logAlteracoes values('{entidade}', '{sistema}', '{tipoValidacao}', '{preValidacao}', '{alteracao}', '{dataAlteracao}', '{tipoAlteracao}');\n"

            banco.cursor().execute(f"{comando}")
            banco.commit()
            banco.close()
        else:
            return False

    def select(self, comando):
        banco = self.conectaBanco()
        dados = banco.cursor()
        a = dados.execute(f"""{comando}""")
        return dados.fetchall()
