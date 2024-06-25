import sys
import pandas as pd
import datetime as dt

from pyodbc import connect, DatabaseError


class Conecta:

    def __init__(self, odbc):
        self.odbc = odbc

    def conectar(self):
        conexao = None
        try:
            conexao = connect(f'DSN={self.odbc}', ConnectionIdleTimeout=0)
            return {'cursor': conexao.cursor(), 'conexao': conexao}
        except (Exception, DatabaseError) as e:
            print(f'\n* Erro ao executar função "conectar". {e}')
            sys.exit()
        # finally:
        #     return {'cursor': conexao.cursor(), 'conexao': conexao}

    def consultar(self, comando):
        conexao = self.conectar()
        lista_dado = []
        try:
            conexao['cursor'].execute(comando)
            resultado = conexao['cursor'].fetchall()
            for i, descricao in enumerate(resultado):
                lista_dado.append({})
                for j, valor in enumerate([d[0] for d in conexao['cursor'].description]):
                    lista_dado[i][valor] = descricao[j]
        except (Exception, DatabaseError) as e:
            print(comando)
            print(f'\n* Erro ao executar função "consultar". {e}')
        finally:
            conexao['cursor'].close()
            return lista_dado

    def executar(self, comando):
        conexao = self.conectar()
        try:
            conexao['cursor'].execute(comando)
        except (Exception, DatabaseError) as e:
            print(comando)
            print(f'\n* Erro ao executar função "executar". {e}')
            return False
        finally:
            conexao['cursor'].close()
        return True

    def executarComLog(self, comando, logAlteracoes, tipoCorrecao, nomeOdbc, sistema, tipoValidacao, preValidacaoBanco, dadoAlterado):
        conexao = self.conectar()
        try:
            conexao['cursor'].execute(comando)
            logAlteracoes.insereLog(tipoAlteracao=tipoCorrecao, entidade=nomeOdbc, sistema=sistema, tipoValidacao=tipoValidacao, preValidacao=preValidacaoBanco, dadoAlterado=dadoAlterado, dataAlteracao=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        except (Exception, DatabaseError) as e:
            print(comando)
            print(f'\n* Erro ao executar função "executar". {e}')
            return False
        finally:
            conexao['cursor'].close()
        return True

    @staticmethod
    def triggerOff(comando, folha=False):
        if folha:
            executar = f"""
                        CALL bethadba.dbp_conn_gera(1, year(today()), 300);
                        set option wait_for_commit = 'on';
                        set option fire_triggers = 'off';
                        {comando}
                        commit; 
                        set option fire_triggers = 'on';
                        """
        else:
            executar = f"""
                        call bethadba.pg_habilitartriggers('off');
                        {comando}
                        commit; 
                        call bethadba.pg_habilitartriggers('on');
                         """
        return executar

    def query(self, query):
        conn = self.conectar()
        result = conn['cursor'].execute(query)
        colunas = [coluna[0] for coluna in result.description]
        df = pd.DataFrame.from_records(result.fetchall(), columns=colunas)
        return df
