from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraModeloFipe
import datetime as dt


def modeloVeiculo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  fipe_nulo=False,
                  marca_desk=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    modeloVeiculoList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_fipe_nulo(pre_validacao):
        nomeValidacao = "Fipe nulo"
        preValidacaoBanco = "fipe_nulo"

        def analisa_fipe_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            modelosAnalisado = []
            for i in modeloVeiculoList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    modelosAnalisado.append(i)

            if len(modelosAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")

            return modelosAnalisado

        def corrige_fipe_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        newModeloFipe = geraModeloFipe()

                        busca = banco.consultar(f"""
                                                select i_fipe
                                                from bethadba.fipe
                                                where descricao = '{dados['i_chave_dsk2']}' and modelo_fipe = null
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                dadoAlterado.append(f"Adicionado o modelo fip {newModeloFipe} para o codigo i_fipe {i['i_fipe']}")
                                comandoUpdate += f"update bethadba.fipe set modelo_fipe = '{newModeloFipe}' where i_fipe in ({i['i_fipe']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_fipe_nulo {e}")

        if fipe_nulo:
            dadoFipeNulo = analisa_fipe_nulo()

            if corrigirErros and len(dadoFipeNulo) > 0:
                corrige_fipe_nulo(listDados=dadoFipeNulo)

    def valida_corrige_marca_desk_nulo(pre_validacao):
        nomeValidacao = "Marca Desk nulo"
        preValidacaoBanco = "marca_desk_nulo"

        def analisa_marca_desk_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            modelosAnalisado = []
            for i in modeloVeiculoList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    modelosAnalisado.append(i)

            if len(modelosAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(modelosAnalisado)}")

            return modelosAnalisado

        def corrige_marca_desk_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        newMarcaDesk = banco.consultar(f"""select first i_marcas from bethadba.marcas""")

                        busca = banco.consultar(f"""
                                                select i_fipe
                                                from bethadba.fipe
                                                where descricao = '{dados['i_chave_dsk2']}' and i_marcas = null
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                dadoAlterado.append(f"Adicionado o a marca {newMarcaDesk[0]['i_marcas']} para o codigo i_fipe {i['i_fipe']}")
                                comandoUpdate += f"update bethadba.fipe set i_marcas = '{newMarcaDesk[0]['i_marcas']}' where i_fipe in ({i['i_fipe']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_marca_desk_nulo {e}")

        if marca_desk:
            dadoCpfNulo = analisa_marca_desk_nulo()

            if corrigirErros and len(dadoCpfNulo) > 0:
                corrige_marca_desk_nulo(listDados=dadoCpfNulo)

    if modeloVeiculoList:
        # valida_corrige_fipe_nulo(pre_validacao='fipe_nulo') desativado pois quando é gerado um valor a informação é migrada errado
        valida_corrige_marca_desk_nulo(pre_validacao='marca_desk')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
