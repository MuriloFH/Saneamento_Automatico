from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def banco(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
          corrigirErros=False,
          banco_fora_padrao=False,
          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_banco_fora_padrao(pre_validacao):
        nomeValidacao = "Código do banco fora do padrão"

        def analisa_banco_fora_padrao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_banco_fora_padrao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                        select i_pessoas_contas, i_pessoas, i_bancos
                                        from bethadba.pessoas_contas
                                        where  i_bancos>=758 and i_bancos !=800
                                        """)

                i_banco = banco.consultar(f"""
                                            SELECT FIRST b.i_bancos, b.nome, situacao
                                            from bethadba.agencias a
                                            join bethadba.bancos b on a.i_bancos = b.i_bancos
                                            where b.nome like '%brasil%'
                                            and b.situacao = 'A'
                                            order by b.i_bancos asc
                                          """)[0]['i_bancos']

                for row in dados:
                    dadoAlterado.append(f"Adicionado o banco {i_banco} para o codigo da pessas_contas {row['i_pessoas_contas']} da pessoa {row['i_pessoas']}")
                    comandoUpdate += f"""UPDATE bethadba.pessoas_contas set i_bancos = {i_banco} where i_pessoas = {row['i_pessoas']} and i_pessoas_contas = {row['i_pessoas_contas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_banco_fora_padrao: {e}")
            return

        if banco_fora_padrao:
            dado = analisa_banco_fora_padrao()

            if corrigirErros and len(dado) > 0:
                corrige_banco_fora_padrao(listDados=dado)

    if dadosList:
        analisa_corrige_banco_fora_padrao(pre_validacao="banco_fora_padrao")

