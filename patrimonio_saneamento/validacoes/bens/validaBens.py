import polars
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraPlacaBem
from datetime import datetime, timedelta


def bens(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
         corrigirErros=False,
         valor_aquisicao_nulo=False,
         valor_depreciado_maior_valor_aquisicao=False,
         valor_depreciado_maior_valor_depreciavel=False,
         dt_depreciacao_menor_dt_aquisicao_bem=False,
         bem_sem_responsavel=False,
         descricao_bem_maior_1024_caracter=False,
         tempo_garantia_negativo=False,
         vida_util_maior_zero_com_depreciacao_anual_igual_zero=False,
         valor_residual_superior_liquido_contabil=False,
         numero_placa_nulo=False,
         placa_duplicada=False
         ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_valor_aquisicao_nulo(pre_validacao):
        nomeValidacao = "Valor de aquisição não informado"

        def analisa_valor_aquisicao_nulo():
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

        def corrige_valor_aquisicao_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            list_i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""select i_bem, valor_aquis, valor_original, valor_depr, valor_depreciavel from bethadba.bens where i_bem in({list_i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        newValorAquisicao = i['valor_depr'] + 10000

                        dadoAlterado.append(f"Adicionado o valor de aquisição {newValorAquisicao} para o bem {i['i_bem']}")

                        comandoUpdate += f"UPDATE bethadba.bens set valor_aquis = {newValorAquisicao}, valor_original = {newValorAquisicao} where i_bem = {i['i_bem']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_valor_aquisicao_nulo: {e}")
            return

        if valor_aquisicao_nulo:
            dado = analisa_valor_aquisicao_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_valor_aquisicao_nulo(listDados=dado)

    def analisa_corrige_valor_depreciado_maior_valor_aquisicao(pre_validacao):
        nomeValidacao = "Valor depreciado maior que valor de aquisição"

        def analisa_valor_depreciado_maior_valor_aquisicao():
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

        def corrige_valor_depreciado_maior_valor_aquisicao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            list_i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""select i_bem, valor_aquis, valor_original, valor_depr, valor_depreciavel from bethadba.bens where i_bem in({list_i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        newValorDepreciado = float(i['valor_aquis']) - 0.01

                        dadoAlterado.append(f"Alterado o valor depreciado {newValorDepreciado} para o bem {i['i_bem']}")

                        comandoUpdate += f"UPDATE bethadba.bens set valor_depr = '{newValorDepreciado}' where i_bem = {i['i_bem']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_valor_depreciado_maior_valor_aquisicao: {e}")
            return

        if valor_depreciado_maior_valor_aquisicao:
            dado = analisa_valor_depreciado_maior_valor_aquisicao()

            if corrigirErros and len(dado) > 0:
                corrige_valor_depreciado_maior_valor_aquisicao(listDados=dado)

    def analisa_corrige_valor_depreciado_maior_valor_depreciavel(pre_validacao):
        nomeValidacao = "Valor depreciado maior que valor depreciavel"

        def analisa_valor_depreciado_maior_valor_depreciavel():
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

        def corrige_valor_depreciado_maior_valor_depreciavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            list_i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""select i_bem, valor_aquis, valor_original, valor_depr, valor_depreciavel from bethadba.bens where i_bem in({list_i_bens})""")

                if len(busca) > 0:
                    for i in busca:

                        if i['valor_depreciavel'] != 0:
                            newValorDepreciado = float(i['valor_depreciavel']) - 0.01
                        else:
                            newValorDepreciado = 0

                        dadoAlterado.append(f"Alterado o valor depreciado {newValorDepreciado} para o bem {i['i_bem']}")

                        comandoUpdate += f"UPDATE bethadba.bens set valor_depr = '{newValorDepreciado}' where i_bem = {i['i_bem']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_valor_depreciado_maior_valor_depreciavel: {e}")
            return

        if valor_depreciado_maior_valor_depreciavel:
            dado = analisa_valor_depreciado_maior_valor_depreciavel()

            if corrigirErros and len(dado) > 0:
                corrige_valor_depreciado_maior_valor_depreciavel(listDados=dado)

    def analisa_corrige_dt_depreciacao_menor_dt_aquisicao_bem(pre_validacao):
        nomeValidacao = "Data de inicio da depreciação do bem menor que data de aquisição"

        def analisa_dt_depreciacao_menor_dt_aquisicao_bem():
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

        def corrige_dt_depreciacao_menor_dt_aquisicao_bem(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            list_i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""select i_bem, dt_inicio_deprec, data_aquis from bethadba.bens where i_bem in({list_i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        data = i['data_aquis']
                        data += timedelta(days=1)
                        newDataDepreciacao = datetime.strftime(data, '%Y-%m-%d')

                        dadoAlterado.append(f"Alterado a data de inicio de depreciação do bem {i['i_bem']} para {newDataDepreciacao}")

                        comandoUpdate += f"UPDATE bethadba.bens set dt_inicio_deprec = '{newDataDepreciacao}' where i_bem = {i['i_bem']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_depreciacao_menor_dt_aquisicao_bem: {e}")
            return

        if dt_depreciacao_menor_dt_aquisicao_bem:
            dado = analisa_dt_depreciacao_menor_dt_aquisicao_bem()

            if corrigirErros and len(dado) > 0:
                corrige_dt_depreciacao_menor_dt_aquisicao_bem(listDados=dado)

    def analisa_corrige_bem_sem_responsavel(pre_validacao):
        nomeValidacao = "Bem sem responsável informado"

        def analisa_bem_sem_responsavel():
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

        def corrige_bem_sem_responsavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            i_bens = ', '.join(list_i_bens)

            try:
                newResponsavel = banco.consultar(f"""SELECT first r.i_respons from bethadba.responsaveis r order by r.i_respons""")

                for i in list_i_bens:
                    dadoAlterado.append(f"Adicionado o responsável {newResponsavel[0]['i_respons']} para o bem {i}")

                comandoUpdate += f"UPDATE bethadba.bens set i_respons = '{newResponsavel[0]['i_respons']}' where i_bem in ({i_bens});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_bem_sem_responsavel: {e}")
            return

        if bem_sem_responsavel:
            dado = analisa_bem_sem_responsavel()

            if corrigirErros and len(dado) > 0:
                corrige_bem_sem_responsavel(listDados=dado)

    def analisa_corrige_descricao_bem_maior_1024_caracter(pre_validacao):
        nomeValidacao = "Descrição de bem maior que 1024 caracteres"

        def analisa_descricao_bem_maior_1024_caracter():
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

        def corrige_descricao_bem_maior_1024_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""select i_bem, descricao from bethadba.bens where i_bem in({i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        newDescricao = i['descricao'][:1023]

                        dadoAlterado.append(f"Alterado a descrição do bem {i['i_bem']} para {newDescricao}")

                        comandoUpdate += f"UPDATE bethadba.bens set descricao = '{newDescricao}' where i_bem in ({i['i_bem']});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_bem_maior_1024_caracter: {e}")
            return

        if descricao_bem_maior_1024_caracter:
            dado = analisa_descricao_bem_maior_1024_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_bem_maior_1024_caracter(listDados=dado)

    def analisa_corrige_tempo_garantia_negativo(pre_validacao):
        nomeValidacao = "Tempo de garantia do bem negativo"

        def analisa_tempo_garantia_negativo():
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

        def corrige_tempo_garantia_negativo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""SELECT i_bem, data_aquis, data_garant from bethadba.bens where i_bem in({i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        newDataGarantia = i['data_aquis']
                        newDataGarantia += timedelta(days=365)
                        newDataGarantia = datetime.strftime(newDataGarantia, '%Y-%m-%d')

                        dadoAlterado.append(f"Alterado a data de garantia do bem {i['i_bem']} para {newDataGarantia}")

                        comandoUpdate += f"UPDATE bethadba.bens set data_garant = '{newDataGarantia}' where i_bem in ({i['i_bem']});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tempo_garantia_negativo: {e}")
            return

        if tempo_garantia_negativo:
            dado = analisa_tempo_garantia_negativo()

            if corrigirErros and len(dado) > 0:
                corrige_tempo_garantia_negativo(listDados=dado)

    def analisa_corrige_vida_util_maior_zero_com_depreciacao_anual_igual_zero(pre_validacao):
        nomeValidacao = "Vita utíl maior que zero com depreciação anual igual a zero"

        def analisa_vida_util_maior_zero_com_depreciacao_anual_igual_zero():
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

        def corrige_vida_util_maior_zero_com_depreciacao_anual_igual_zero(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])

            i_bens = ', '.join(list_i_bens)

            try:
                busca = banco.consultar(f"""SELECT i_bem, data_aquis, data_garant from bethadba.bens where i_bem in({i_bens})""")

                if len(busca) > 0:
                    for i in busca:
                        newDataGarantia = i['data_aquis']
                        newDataGarantia += timedelta(days=365)
                        newDataGarantia = datetime.strftime(newDataGarantia, '%Y-%m-%d')

                        dadoAlterado.append(f"Alterado a data de garantia do bem {i['i_bem']} para {newDataGarantia}")

                        comandoUpdate += f"UPDATE bethadba.bens set data_garant = '{newDataGarantia}' where i_bem in ({i['i_bem']});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_vida_util_maior_zero_com_depreciacao_anual_igual_zero: {e}")
            return

        if vida_util_maior_zero_com_depreciacao_anual_igual_zero:
            dado = analisa_vida_util_maior_zero_com_depreciacao_anual_igual_zero()

            if corrigirErros and len(dado) > 0:
                corrige_vida_util_maior_zero_com_depreciacao_anual_igual_zero(listDados=dado)

    def analisa_corrige_valor_residual_superior_liquido_contabil(pre_validacao):
        nomeValidacao = "Valor residual superior ao valor liquido contábil"

        def analisa_valor_residual_superior_liquido_contabil():
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

        def corrige_valor_residual_superior_liquido_contabil(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoDelete = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")
            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])
            i_bens = ', '.join(list_i_bens)

            try:
                bensConsulta = banco.consultar(f"""
                                                SELECT b.i_bem, b.valor_aquis, b.valor_residual, b.valor_depreciavel, b.valor_depr, bethadba.dbf_valor_bem(b.i_entidades,b.i_bem) as valorLiquidoContabil
                                                from bethadba.bens b 
                                                where b.valor_residual > valorLiquidoContabil and b.i_bem in ({i_bens})
                                                """)

                for bem in bensConsulta:
                    # zerando os valores residuais dos bens
                    dadoAlterado.append(f"Definido o valor residual para 0 (zero) no bem {bem['i_bem']}")
                    comandoUpdate += f"update bethadba.bens set valor_residual = 0 where i_entidades = 1 and i_bem in ({bem['i_bem']}); \n"

                    if bem['valorLiquidoContabil'] < 0:
                        # Para valor líquido contábil negativo, usados os seguintes updates, fazendo com que fique zerado:
                        dadoAlterado.append(f"Definido valor liquido contábil para 0 (zero) pois o bem {bem['i_bem']} possui o valor negativo")

                        histPrincipal = banco.consultar(f"""SELECT first i_hist_bens_contas
                                                            from bethadba.hist_bens_contas hbc 
                                                            where hbc.i_bem = {bem['i_bem']} and hbc.tipo in (5, 7, 11, 12,13, 14, 16)
                                                            order by hbc.data_mov DESC
                                                        """)[0]['i_hist_bens_contas']

                        comandoUpdate += f"""UPDATE bethadba.hist_bens_contas h1 join bethadba.hist_bens_contas h2 on (h1.i_bem = h2.i_bem) set h1.valor = h2.valor where h1.i_hist_bens_contas = {histPrincipal} and h2.tipo = 1;\n"""

                        comandoDelete += f"""DELETE from bethadba.hist_bens_contas where i_bem = {bem['i_bem']} and i_hist_bens_contas != {histPrincipal} and tipo in (5, 7, 11, 12,13, 14, 16);\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)
                banco.executar(comando=banco.triggerOff(comandoDelete))

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_valor_residual_superior_liquido_contabil: {e}")
            return

        if valor_residual_superior_liquido_contabil:
            dado = analisa_valor_residual_superior_liquido_contabil()

            if corrigirErros and len(dado) > 0:
                corrige_valor_residual_superior_liquido_contabil(listDados=dado)

    def analisa_corrige_numero_placa_nulo(pre_validacao):
        nomeValidacao = "Numero de placa nulo"

        def analisa_numero_placa_nulo():
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

        def corrige_numero_placa_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            for a in listDados:
                list_i_bens.append(a['i_chave_dsk2'])
            i_bens = ', '.join(list_i_bens)

            busca = banco.consultar(f"""SELECT b.i_bem, b.data_aquis from bethadba.bens b  where i_bem in({i_bens})""")

            try:
                for i in busca:
                    newPlaca = geraPlacaBem(idBem=i['i_bem'], dtAquisicao=i['data_aquis'])

                    dadoAlterado.append(f"Inserido a placa {newPlaca} para o bem {i['i_bem']}")

                    comandoUpdate += f"UPDATE bethadba.bens set numero_placa = '{newPlaca}' where i_bem in ({i['i_bem']});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")

            except Exception as e:
                print(f"Erro na função corrige_numero_placa_nulo: {e}")

        if numero_placa_nulo:
            dado = analisa_numero_placa_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_numero_placa_nulo(listDados=dado)

    def analisa_corrige_placa_duplicada(pre_validacao):
        nomeValidacao = "Numero de placa duplicado"

        def analisa_placa_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}'")

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

        def corrige_placa_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            list_i_bens = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            busca = banco.consultar(f"""SELECT list(b.i_bem) as i_bem, count(b.numero_placa) as numero_placa
                                        from bethadba.bens b
                                        group by b.numero_placa
                                        HAVING count(b.numero_placa) > 1""")

            try:
                for i in busca:
                    i_bem = i['i_bem'].split(',')

                    for bem in i_bem:
                        newPlaca = geraPlacaBem(bem, '1900-01-01')

                        dadoAlterado.append(f"Inserido a placa {newPlaca} para o bem {bem}")

                        comandoUpdate += f"UPDATE bethadba.bens set numero_placa = '{newPlaca}' where i_bem in ({bem});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")

            except Exception as e:
                print(f"Erro na função corrige_numero_placa_nulo: {e}")

        if placa_duplicada:
            dado = analisa_placa_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_placa_duplicada(listDados=dado)

    if dadosList:
        analisa_corrige_valor_aquisicao_nulo(pre_validacao="valor_aquisicao_nulo")
        analisa_corrige_valor_depreciado_maior_valor_aquisicao(pre_validacao="valor_depreciado_maior_valor_aquisicao")
        analisa_corrige_valor_depreciado_maior_valor_depreciavel(pre_validacao="valor_depreciado_maior_valor_depreciavel")
        analisa_corrige_dt_depreciacao_menor_dt_aquisicao_bem(pre_validacao="dt_depreciacao_menor_dt_aquisicao_bem")
        analisa_corrige_bem_sem_responsavel(pre_validacao="bem_sem_responsavel")
        analisa_corrige_descricao_bem_maior_1024_caracter(pre_validacao="descricao_bem_maior_1024_caracter")
        analisa_corrige_tempo_garantia_negativo(pre_validacao="tempo_garantia_negativo")
        analisa_corrige_vida_util_maior_zero_com_depreciacao_anual_igual_zero(pre_validacao="vida_util_maior_zero_com_depreciacao_anual_igual_zero")
        analisa_corrige_valor_residual_superior_liquido_contabil(pre_validacao="valor_residual_superior_liquido_contabil")
        analisa_corrige_numero_placa_nulo(pre_validacao="numero_placa_nulo")
        analisa_corrige_placa_duplicada(pre_validacao="placa_duplicada")
