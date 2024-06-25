from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraPlacaBem, remove_caracteres_especiais
from datetime import datetime, timedelta


def baixas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
           corrigirErros=False,
           historico_nulo=False,
           motivo_nulo=False,
           bem_placa_nulo=False,
           data_baixa_superior_data_aquisicao_bem=False,
           numero_boletim_ocorrencia_maior_oito_caracteres=False,
           numero_processo_maior_oito_caracteres=False,
           numero_processo_caracter_especial=False,
           numero_boletim_ocorrencia_caracter_especial=False,
           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_historico_nulo(pre_validacao):
        nomeValidacao = "Historico nulo"

        def analisa_historico_nulo():
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

        def corrige_historico_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    dadoAlterado.append(f"Adicionado historico para a baixa {i['i_chave_dsk2']}")
                    comandoUpdate += f"""update bethadba.baixas set historico = 'Motivo não declarado' where i_baixa in({i['i_chave_dsk2']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_historico_nulo: {e}")
            return

        if historico_nulo:
            dado = analisa_historico_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_historico_nulo(listDados=dado)

    def analisa_corrige_motivo_nulo(pre_validacao):
        nomeValidacao = "Motivo nulo"

        def analisa_motivo_nulo():
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

        def corrige_motivo_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                newMotivo = banco.consultar(f"SELECT FIRST i_motivo from bethadba.motivos m order by m.i_motivo")

                for i in listDados:
                    dadoAlterado.append(f"Adicionado o motivo para a baixa {i['i_chave_dsk2']}")
                    comandoUpdate += f"""update bethadba.baixas set i_motivo = {newMotivo} where i_baixa in({i['i_chave_dsk2']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_motivo_nulo: {e}")
            return

        if motivo_nulo:
            dado = analisa_motivo_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_motivo_nulo(listDados=dado)

    def analisa_corrige_bem_placa_nulo(pre_validacao):
        nomeValidacao = "Bem com placa nulo"

        def analisa_bem_placa_nulo():
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

        def corrige_bem_placa_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    bem = banco.consultar(f"""SELECT b2.i_bem, b2.data_aquis
                                                from bethadba.baixas b 
                                                join bethadba.bens b2 on (b.i_bem = b2.i_bem)
                                                where b.i_baixa = {i['i_chave_dsk2']};
                                            """)

                    newPlaca = geraPlacaBem(idBem=bem[0]['i_bem'], dtAquisicao=bem[0]['data_aquis'])

                    dadoAlterado.append(f"Adicionado a placa {newPlaca} do bem {bem[0]['i_bem']}")
                    comandoUpdate += f"""update bethadba.bens set numero_placa = {newPlaca} where i_bem in({bem[0]['i_bem']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_bem_placa_nulo: {e}")
            return

        if bem_placa_nulo:
            dado = analisa_bem_placa_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_bem_placa_nulo(listDados=dado)

    def analisa_corrige_data_baixa_superior_data_aquisicao_bem(pre_validacao):
        nomeValidacao = "Data da baixa superior a data de aquisição do bem"

        def analisa_data_baixa_superior_data_aquisicao_bem():
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

        def corrige_data_baixa_superior_data_aquisicao_bem(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    bem = banco.consultar(f"""SELECT b.i_baixa, b.data_baixa, b2.i_bem, b2.data_aquis
                                                from bethadba.baixas b 
                                                join bethadba.bens b2 on (b.i_bem = b2.i_bem)
                                                where b.i_baixa = {i['i_chave_dsk2']};
                                            """)

                    newDataBaixa = datetime.strptime(str(bem[0]['data_aquis']), '%Y-%m-%d')
                    newDataBaixa += timedelta(days=30)

                    dadoAlterado.append(f"Alterado a data da baixa para {newDataBaixa} da baixa {i['i_chave_dsk2']}")
                    comandoUpdate += f"""update bethadba.baixas set data_baixa = '{newDataBaixa}' where i_baixa in({i['i_chave_dsk2']});\n"""
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_baixa_superior_data_aquisicao_bem: {e}")
            return

        if data_baixa_superior_data_aquisicao_bem:
            dado = analisa_data_baixa_superior_data_aquisicao_bem()

            if corrigirErros and len(dado) > 0:
                corrige_data_baixa_superior_data_aquisicao_bem(listDados=dado)

    def analisa_corrige_numero_boletim_ocorrencia_maior_oito_caracteres(pre_validacao):
        nomeValidacao = "Boletim de Ocorrência maior que 8 caracteres"

        def analisa_numero_boletim_ocorrencia_maior_oito_caracteres():
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

        def corrige_numero_boletim_ocorrencia_maior_oito_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            dados = []
            list_i_baixas = []

            print(f">> Iniciando a correção '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            for baixa in listDados:
                dados.append(baixa['i_chave_dsk2'])
                list_i_baixas = ', '.join(dados)

            busca = banco.consultar(f"""SELECT b.i_baixa, b.nro_bo from bethadba.baixas b where b.i_baixa in ({list_i_baixas})""")

            try:
                for i in busca:
                    newNumero = i['nro_bo'][:8]

                    dadoAlterado.append(f"Alterado o boletim de ocorrência da baixa {i['i_baixa']} para {newNumero}")
                    comandoUpdate += f"""update bethadba.baixas set nro_bo = '{newNumero}' where i_baixa = {i['i_baixa']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_boletim_ocorrencia_maior_oito_caracteres: {e}")
            return

        if numero_boletim_ocorrencia_maior_oito_caracteres:
            dado = analisa_numero_boletim_ocorrencia_maior_oito_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_numero_boletim_ocorrencia_maior_oito_caracteres(listDados=dado)

    def analisa_corrige_numero_processo_maior_oito_caracteres(pre_validacao):
        nomeValidacao = "Numero do processo administrativo maior que 8 caracteres"

        def analisa_numero_processo_maior_oito_caracteres():
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

        def corrige_numero_processo_maior_oito_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            dados = []
            list_i_baixas = []

            print(f">> Iniciando a correção '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            for baixa in listDados:
                dados.append(baixa['i_chave_dsk2'])
                list_i_baixas = ', '.join(dados)

            busca = banco.consultar(f"""SELECT b.i_baixa, b.nr_proc_adm from bethadba.baixas b where b.i_baixa in ({list_i_baixas})""")

            try:
                for i in busca:
                    newNumero = i['nr_proc_adm'][:8]

                    dadoAlterado.append(f"Alterado o numero do processo administrativo da baixa {i['i_baixa']} para {newNumero}")
                    comandoUpdate += f"""update bethadba.baixas set nr_proc_adm = '{newNumero}' where i_baixa = {i['i_baixa']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_processo_maior_oito_caracteres: {e}")
            return

        if numero_processo_maior_oito_caracteres:
            dado = analisa_numero_processo_maior_oito_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_numero_processo_maior_oito_caracteres(listDados=dado)

    def analisa_corrige_numero_processo_letras_caracter_especial(pre_validacao):
        nomeValidacao = "Numero do processo administrativo com caracter especial ou letras"

        def analisa_numero_processo_letras_caracter_especial():
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

        def corrige_numero_processo_letras_caracter_especial(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            dados = []
            list_i_baixas = []

            print(f">> Iniciando a correção '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            for baixa in listDados:
                dados.append(baixa['i_chave_dsk2'])
                list_i_baixas = ', '.join(dados)

            busca = banco.consultar(f"""SELECT b.i_baixa, b.nr_proc_adm from bethadba.baixas b where b.i_baixa in ({list_i_baixas})""")

            try:
                for i in busca:
                    newNumero = remove_caracteres_especiais(i['nr_proc_adm'])

                    dadoAlterado.append(f"Alterado o numero do processo administrativo da baixa {i['i_baixa']} para {newNumero}")
                    comandoUpdate += f"""update bethadba.baixas set nr_proc_adm = '{newNumero}' where i_baixa = {i['i_baixa']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_processo_letras_caracter_especial: {e}")
            return

        if numero_processo_caracter_especial:
            dado = analisa_numero_processo_letras_caracter_especial()

            if corrigirErros and len(dado) > 0:
                corrige_numero_processo_letras_caracter_especial(listDados=dado)

    def analisa_corrige_numero_boletim_ocorrencia_caracter_especial(pre_validacao):
        nomeValidacao = "Numero do boletim de ocorrencia com caracter especial"

        def analisa_numero_boletim_ocorrencia_caracter_especial():
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

        def corrige_numero_boletim_ocorrencia_caracter_especial(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            dados = []
            list_i_baixas = []

            print(f">> Iniciando a correção '{nomeValidacao}'")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            for baixa in listDados:
                dados.append(baixa['i_chave_dsk2'])
                list_i_baixas = ', '.join(dados)

            busca = banco.consultar(f"""SELECT b.i_baixa, b.nro_bo from bethadba.baixas b where b.i_baixa in ({list_i_baixas})""")

            try:
                for i in busca:
                    newNumero = remove_caracteres_especiais(i['nro_bo'])

                    dadoAlterado.append(f"Alterado o numero do processo administrativo da baixa {i['i_baixa']} para {newNumero}")
                    comandoUpdate += f"""update bethadba.baixas set nro_bo = '{newNumero}' where i_baixa = {i['i_baixa']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_boletim_ocorrencia_caracter_especial: {e}")
            return

        if numero_boletim_ocorrencia_caracter_especial:
            dado = analisa_numero_boletim_ocorrencia_caracter_especial()

            if corrigirErros and len(dado) > 0:
                corrige_numero_boletim_ocorrencia_caracter_especial(listDados=dado)

    if dadosList:
        analisa_corrige_historico_nulo(pre_validacao='historico_nulo')
        analisa_corrige_motivo_nulo(pre_validacao='motivo_nulo')
        analisa_corrige_bem_placa_nulo(pre_validacao="bem_placa_nulo")
        analisa_corrige_data_baixa_superior_data_aquisicao_bem(pre_validacao="data_baixa_superior_data_aquisicao_bem")
        analisa_corrige_numero_boletim_ocorrencia_maior_oito_caracteres(pre_validacao="numero_boletim_ocorrencia_maior_oito_caracteres")
        analisa_corrige_numero_processo_maior_oito_caracteres(pre_validacao="numero_processo_maior_oito_caracteres")
        analisa_corrige_numero_processo_letras_caracter_especial(pre_validacao="numero_processo_letras_caracter_especial")
        analisa_corrige_numero_boletim_ocorrencia_caracter_especial(pre_validacao="numero_boletim_ocorrencia_caracter_especial")
