from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def veiculosPublicacao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       processo_compra_nulo=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_processo_compra_nulo(pre_validacao):
        nomeValidacao = "Veiculo de publicação do tipo 'Empresa Contratada' sem processo de compra informado"

        def analisa_processo_compra_nulo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_compra_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                processo = banco.consultar(""" SELECT FIRST p.i_ano_proc, p.i_processo from compras.compras.processos p order by p.i_ano_proc desc""")

                if len(listDados) > 0:
                    for row in listDados:
                        comandoUpdate += f"""UPDATE compras.compras.orgaos_publica set i_processo = {processo[0]['i_processo']}, i_ano_proc = {processo[0]['i_ano_proc']} where i_orgaos_publica = {row['i_chave_dsk2']} and i_entidades = {row['i_chave_dsk1']};\n"""
                        dadoAlterado.append(f"Adicionado o processo {processo[0]['i_processo']}/{processo[0]['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_compra_nulo: {e}")
            return

        if processo_compra_nulo:
            dado = analisa_processo_compra_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_processo_compra_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_processo_compra_nulo(pre_validacao="processo_compra_nulo")
