from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraRenavam, geraChassi, geraNumeroPatrimonio
import datetime as dt


def veiculo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
            corrigirErros=False,
            renavan_duplicado=False,
            chassi_duplicado=False,
            patrimonio_duplicado=False,
            chassi_maior_20_caracter=False,
            consumo_maximo_maior_99=False,
            consumo_minimo_maior_99=False,
            dt_aquisicao_menor_dt_fabricacao=False,
            renavam_invalido=False,
            veiculo_proprio_fornecedor=False,
            ano_fabricacao_invalido=False
            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = tipo_registro

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_dt_aquisicao_menor_1900():  # dependencia da função valida_corrige_dt_aquisicao_menor_dt_fabricacao() e valida_corrige_ano_fabricacao_invalido()
        nomeValidacao = "Data de aquisição menor que  1900"
        preValidacaoBanco = "dt_aquisicao_menor_1900"

        def analisa_dt_aquisicao_menor_1900():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            busca = banco.consultar(f"""
                                    SELECT i_veiculos, data_aquisicao, v.ano
                                    from bethadba.veiculos v 
                                    where (v.data_aquisicao is null or v.data_aquisicao < '1900-01-01')
                                    """)

            if len(busca) > 0:
                print(f">> Total de inconsistências encontradas: {len(busca)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(busca)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(busca)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(busca)}")

            return busca

        def corrige_dt_aquisicao_menor_1900(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:

                    for dados in listDados:
                        if dados['ano'] is None:
                            newAnoAquisicao = f"1901-01-01"
                        else:
                            newAnoAquisicao = f"{dados['ano']}-01-01"

                        dadoAlterado.append(f"Alterado o ano de aquisicao para {newAnoAquisicao} do veiculo com a placa {dados['i_veiculos']}")
                        comandoUpdate += f"update bethadba.veiculos set data_aquisicao = '{newAnoAquisicao}' where i_veiculos in ('{dados['i_veiculos']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=preValidacaoBanco, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_dt_aquisicao_menor_1900 {e}")

        if dt_aquisicao_menor_dt_fabricacao:
            dado = analisa_dt_aquisicao_menor_1900()

            if corrigirErros and len(dado) > 0:
                corrige_dt_aquisicao_menor_1900(listDados=dado)

    def valida_corrige_renavam_duplicado(pre_validacao):
        nomeValidacao = "Renavam duplicado"
        preValidacaoBanco = "renavam_duplicado"

        def analisa_renavam_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            listTratado = []
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

        def corrige_renavam_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f"""
                                            SELECT list(v.i_veiculos) as i_veiculos,
                                            v.renavam,
                                            count(v.renavam) cont
                                            from bethadba.veiculos v 
                                            GROUP by v.renavam
                                            having cont > 1
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            list_i_veiculos = i['i_veiculos'].split(',')
                            if len(list_i_veiculos) > 0:
                                for i_veiculos in list_i_veiculos[1:]:  # feito o [1:] para preservar o primeiro veiculo coletado com o renavam já presente na base
                                    newRenavam = geraRenavam()

                                    dadoAlterado.append(f"Adicionado o numero de renavam {newRenavam} para o veiculo com a placa {i_veiculos}")
                                    comandoUpdate += f"update bethadba.veiculos set renavam = '{newRenavam}' where i_veiculos in ('{i_veiculos}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_renavam_duplicado {e}")

        if renavan_duplicado:
            dado = analisa_renavam_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_renavam_duplicado(listDados=dado)

    def valida_corrige_chassi_duplicado(pre_validacao):
        nomeValidacao = "Chassi duplicado"
        preValidacaoBanco = pre_validacao

        def analisa_chassi_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            listTratado = []
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

        def corrige_chassi_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f"""
                                            SELECT list(v.i_veiculos) as i_veiculos,
                                            v.chassi,
                                            count(v.chassi) cont
                                            from bethadba.veiculos v 
                                            GROUP by v.chassi
                                            having cont > 1
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            list_i_veiculos = i['i_veiculos'].split(',')
                            if len(list_i_veiculos) > 0:
                                for i_veiculos in list_i_veiculos[1:]:  # feito o [1:] para preservar o primeiro veiculo coletado com o chassi já presente na base
                                    newChassi = geraChassi()

                                    dadoAlterado.append(f"Adicionado o chassi {newChassi} para o veiculo com a placa {i_veiculos}")
                                    comandoUpdate += f"update bethadba.veiculos set chassi = '{newChassi}' where i_veiculos in ('{i_veiculos}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_chassi_duplicado {e}")

        if chassi_duplicado:
            dado = analisa_chassi_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_chassi_duplicado(listDados=dado)

    def valida_corrige_patrimonio_duplicado(pre_validacao):
        nomeValidacao = "Patrimonio duplicado"
        preValidacaoBanco = pre_validacao

        def analisa_patrimonio_duplicado():
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

        def corrige_patrimonio_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f"""
                                            SELECT list(v.i_veiculos) as i_veiculos,
                                            v.reg_patrim,
                                            count(v.reg_patrim) cont
                                            from bethadba.veiculos v 
                                            GROUP by v.reg_patrim 
                                            having cont > 1
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            list_i_veiculos = i['i_veiculos'].split(',')
                            if len(list_i_veiculos) > 0:
                                for i_veiculos in list_i_veiculos[1:]:  # feito o [1:] para preservar o primeiro veiculo coletado com o patrimonio já presente na base
                                    newPatrimonio = geraNumeroPatrimonio()

                                    dadoAlterado.append(f"Adicionado o patrimonio {newPatrimonio} para o veiculo com a placa {i_veiculos}")
                                    comandoUpdate += f"update bethadba.veiculos set reg_patrim = '{newPatrimonio}' where i_veiculos in ('{i_veiculos}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_patrimonio_duplicado {e}")

        if patrimonio_duplicado:
            dado = analisa_patrimonio_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_patrimonio_duplicado(listDados=dado)

    def valida_corrige_chassi_maior_20_caracter(pre_validacao):
        nomeValidacao = "Chassi maior que 20 caracteres"
        preValidacaoBanco = pre_validacao

        def analisa_chassi_maior_20_caracter():
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

        def corrige_chassi_maior_20_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                select i_veiculos,
                                                chassi
                                                from bethadba.veiculos
                                                where i_veiculos = '{dados['chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            chassiAjustado = busca[0]['chassi'][:20]

                            dadoAlterado.append(f"Ajustado o chassi {chassiAjustado} para o veiculo com a placa {dados[0]['i_veiculos']}")
                            comandoUpdate += f"update bethadba.veiculos set chassi = '{chassiAjustado}' where i_veiculos in ('{dados[0]['i_veiculos']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_chassi_maior_20_caracter {e}")

        if chassi_maior_20_caracter:
            dado = analisa_chassi_maior_20_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_chassi_maior_20_caracter(listDados=dado)

    def valida_corrige_consumo_maximo_maior_99(pre_validacao):
        nomeValidacao = "Consumo máximo maior que 99.99"
        preValidacaoBanco = pre_validacao

        def analisa_consumo_maximo_maior_99():
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

        def corrige_consumo_maximo_maior_99(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                placaVeiculo = []
                try:
                    consumoAjustado = 99.00

                    for dados in listDados:
                        print(dados)
                        placaVeiculo.append(f"'{dados['chave_dsk2']}'")

                        dadoAlterado.append(f"Ajustado o consumo máximo para {consumoAjustado} do veiculo com a placa {dados['chave_dsk2']}")

                    placaVeiculo = ",".join(placaVeiculo)
                    comandoUpdate += f"update bethadba.veiculos set cons_maximo = {consumoAjustado} where i_veiculos in ({placaVeiculo});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_consumo_maximo_maior_99 {e}")

        if consumo_maximo_maior_99:
            dado = analisa_consumo_maximo_maior_99()

            if corrigirErros and len(dado) > 0:
                corrige_consumo_maximo_maior_99(listDados=dado)

    def valida_corrige_consumo_minimo_maior_99(pre_validacao):
        nomeValidacao = "Consumo minimo maior que 99.99"
        preValidacaoBanco = pre_validacao

        def analisa_consumo_minimo_maior_99():
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

        def corrige_consumo_minimo_maior_99(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                placaVeiculo = []
                try:
                    consumoAjustado = 99.00

                    for dados in listDados:
                        placaVeiculo.append(f"'{dados['chave_dsk2']}'")

                        dadoAlterado.append(f"Ajustado o consumo minimo para {consumoAjustado} do veiculo com a placa {dados['chave_dsk2']}")

                    placaVeiculo = ",".join(placaVeiculo)
                    comandoUpdate += f"update bethadba.veiculos set cons_minimo = {consumoAjustado} where i_veiculos in ({placaVeiculo});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_consumo_minimo_maior_99 {e}")

        if consumo_minimo_maior_99:
            dado = analisa_consumo_minimo_maior_99()

            if corrigirErros and len(dado) > 0:
                corrige_consumo_minimo_maior_99(listDados=dado)

    def valida_corrige_dt_aquisicao_menor_dt_fabricacao(pre_validacao):
        nomeValidacao = "Data de aquisição menor que data de fabricação"
        preValidacaoBanco = "dt_aquisicao_menor_dt_fabricacao"

        def analisa_dt_aquisicao_menor_dt_fabricacao():
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

        def corrige_dt_aquisicao_menor_dt_fabricacao(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:

                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                select i_veiculos,
                                                data_aquisicao as dataAquisicao
                                                from bethadba.veiculos
                                                where i_veiculos = '{dados['i_chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                newAnoFabricacao = int(str(i['dataAquisicao'])[:4])

                                dadoAlterado.append(f"Alterado o ano de fabricação para {newAnoFabricacao} do veiculo com a placa {i['i_veiculos']}")
                                comandoUpdate += f"update bethadba.veiculos set ano = {newAnoFabricacao} where i_veiculos in ('{i['i_veiculos']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_dt_aquisicao_menor_dt_fabricacao {e}")

        if dt_aquisicao_menor_dt_fabricacao:
            dado = analisa_dt_aquisicao_menor_dt_fabricacao()

            if corrigirErros and len(dado) > 0:
                corrige_dt_aquisicao_menor_dt_fabricacao(listDados=dado)

    def valida_corrige_renavam_invalido(pre_validacao):
        nomeValidacao = "Renavam inválido"
        preValidacaoBanco = "renavam_invalido"

        def analisa_valida_corrige_renavam_invalido():
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

        def corrige_valida_corrige_renavam_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:

                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                select i_veiculos,
                                                renavam
                                                from bethadba.veiculos
                                                where i_veiculos = {dados['chave_dsk2']}
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                oldRenavam = i['renavam']
                                newRenavam = int(''.join(filter(str.isnumeric, i['renavam'])))

                                dadoAlterado.append(f"Removido os caracteres especiais e letras do renavam {oldRenavam} do veiculo com a placa {i['i_veiculos']}")
                                comandoUpdate += f"update bethadba.veiculos set renavam = {newRenavam} where i_veiculos in ('{i['i_veiculos']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_renavam_invalido {e}")

        if renavam_invalido:
            dado = analisa_valida_corrige_renavam_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_valida_corrige_renavam_invalido(listDados=dado)

    def valida_corrige_veiculo_proprio_fornecedor(pre_validacao):
        nomeValidacao = "Veiculo marcado como proprio com fornecedor informado"
        preValidacaoBanco = "veiculo_proprio_fornecedor"

        def analisa_veiculo_proprio_fornecedor():
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

        def corrige_veiculo_proprio_fornecedor(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:

                    for dados in listDados:
                        dadoAlterado.append(f"Removido o i_fornecedores do veiculo com a placa {dados['chave_dsk2']}")
                        comandoUpdate += f"update bethadba.veiculos set i_fornecedores = null where i_veiculos in ('{dados['chave_dsk2']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_veiculo_proprio_fornecedor {e}")

        if veiculo_proprio_fornecedor:
            dado = analisa_veiculo_proprio_fornecedor()

            if corrigirErros and len(dado) > 0:
                corrige_veiculo_proprio_fornecedor(listDados=dado)

    def valida_corrige_ano_fabricacao_invalido(pre_validacao):
        nomeValidacao = "Ano de fabricação do veiculo inválida"
        preValidacaoBanco = "ano_fabricacao_invalido"

        def analisa_ano_fabricacao_invalido():
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

        def corrige_ano_fabricacao_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:

                    for dados in listDados:
                        busca = banco.consultar(f"""SELECT v.i_veiculos, v.data_aquisicao, v.ano
                                                    from bethadba.veiculos v 
                                                    where v.i_veiculos = '{dados['i_chave_dsk2']}' and v.data_aquisicao is not null
                                                """)

                        newAnoFabricacao = dt.datetime.strftime(busca[0]['data_aquisicao'], '%Y')

                        dadoAlterado.append(f"Inserido o ano de fabricação {newAnoFabricacao} para o veiculo com a placa {dados['i_chave_dsk2']}")
                        comandoUpdate += f"update bethadba.veiculos set ano = '{newAnoFabricacao}' where i_veiculos in ('{dados['i_chave_dsk2']}');\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_ano_fabricacao_invalido {e}")

        if ano_fabricacao_invalido:
            dado = analisa_ano_fabricacao_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_ano_fabricacao_invalido(listDados=dado)

    if dadosList:
        valida_corrige_dt_aquisicao_menor_1900()
        valida_corrige_renavam_duplicado(pre_validacao='renavan_duplicado')
        valida_corrige_chassi_duplicado(pre_validacao='chassi_duplicado')
        valida_corrige_patrimonio_duplicado(pre_validacao='patrimonio_duplicado')
        valida_corrige_chassi_maior_20_caracter(pre_validacao='chassi_maior_20_caracter')
        valida_corrige_consumo_maximo_maior_99(pre_validacao='chassi_maior_20_caracter')
        valida_corrige_consumo_minimo_maior_99(pre_validacao='consumo_maximo_maior_99')
        valida_corrige_dt_aquisicao_menor_dt_fabricacao(pre_validacao='dt_aquisicao_menor_dt_fabricacao')
        valida_corrige_renavam_invalido(pre_validacao='renavam_invalido')
        valida_corrige_veiculo_proprio_fornecedor(pre_validacao='veiculo_proprio_fornecedor')
        valida_corrige_ano_fabricacao_invalido(pre_validacao='ano_fabricacao_invalido')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
