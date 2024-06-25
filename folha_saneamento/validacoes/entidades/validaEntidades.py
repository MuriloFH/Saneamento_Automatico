from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def entidade(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             indicativo_educacional_nulo=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_indicativo_educacional_nulo(pre_validacao):
        nomeValidacao = "Indicativo educacional da entidade vazio"

        def analisa_indicativo_educacional_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_indicativo_educacional_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, indicativo_entidade_educativa, i_competencias
                                            from bethadba.hist_entidades_compl hec 
                                            where indicativo_entidade_educativa is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        print(row)

                        dadoAlterado.append(f"Inserido o indicativo educacional 'N' para o historico da entidade {row['i_entidades']} da competência {row['i_competencias']}")
                        comandoUpdate += f"UPDATE bethadba.hist_entidades_compl set indicativo_entidade_educativa = 'N' where i_entidades = {row['i_entidades']} and i_competencias = '{row['i_competencias']}';\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_indicativo_educacional_nulo: {e}")
            return

        if indicativo_educacional_nulo:
            dado = analisa_indicativo_educacional_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_indicativo_educacional_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_indicativo_educacional_nulo(pre_validacao="indicativo_educacional_nulo")
