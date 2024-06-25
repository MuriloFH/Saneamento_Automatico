from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from datetime import datetime


def manutencaoBem(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  codigo_bem_nulo=False,
                  data_envio_manutencao_nulo=False,
                  prestador_servico_nulo=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_codigo_bem_obrigatorio(pre_validacao):
        nomeValidacao = "Código do bem vazio"

        def analisa_codigo_bem_obrigatorio():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_bem_obrigatorio(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                bem = banco.consultar(f"""SELECT first i_bem from bethadba.bens b order by b.i_bem;""")
                i_bem = bem[0]['i_bem']

                for i in listDados:
                    dadoAlterado.append(f"Adicionado o bem {i_bem} na manutenção {i['i_chave_dsk1']}")

                    comandoUpdate += f"update bethadba.manutencoes set i_bem = {i_bem} where i_manutencao = {i['i_chave_dsk1']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_codigo_bem_obrigatorio: {e}")
            return

        if codigo_bem_nulo:
            dado = analisa_codigo_bem_obrigatorio()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_bem_obrigatorio(listDados=dado)

    def analisa_corrige_data_envio_manutencao_nulo(pre_validacao):
        nomeValidacao = "Data de envio da manutenção vazio"

        def analisa_data_envio_manutencao_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_envio_manutencao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    newData = datetime.now()
                    newData = newData.strftime('%Y-%m-%d')

                    dadoAlterado.append(f"Adicionado a data {newData} na manutenção {i['i_chave_dsk1']}")

                    comandoUpdate += f"update bethadba.manutencoes set data_env = '{newData}' where i_manutencao = {i['i_chave_dsk1']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_envio_manutencao_nulo: {e}")
            return

        if codigo_bem_nulo:
            dado = analisa_data_envio_manutencao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_data_envio_manutencao_nulo(listDados=dado)

    def analisa_corrige_prestador_servico_nulo(pre_validacao):
        nomeValidacao = "Prestador de serviço não informado"

        def analisa_prestador_servico_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_prestador_servico_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                newPrestadorServico = banco.consultar(f"select first i_fornec from bethadba.fornecedores order by i_fornec")
                newPrestadorServico = newPrestadorServico[0]['i_fornec']

                for i in listDados:
                    dadoAlterado.append(f"Adicionado o prestador de serviço {newPrestadorServico} na manutenção {i['i_chave_dsk1']}")

                    comandoUpdate += f"update bethadba.manutencoes set i_fornec = '{newPrestadorServico}' where i_manutencao = {i['i_chave_dsk1']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_prestador_servico_nulo: {e}")
            return

        if codigo_bem_nulo:
            dado = analisa_prestador_servico_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_prestador_servico_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_codigo_bem_obrigatorio(pre_validacao="codigo_bem_obrigatorio")
        analisa_corrige_data_envio_manutencao_nulo(pre_validacao="data_envio_manutencao")
        analisa_corrige_prestador_servico_nulo(pre_validacao="prestador_servico_nulo")
