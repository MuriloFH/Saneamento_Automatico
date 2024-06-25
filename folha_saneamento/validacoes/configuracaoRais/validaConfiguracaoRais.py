from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import colorama


def configuracaoRais(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                     corrigirErros=False,
                     controle_ponto_nulo=False,
                     inscricao_nulo_ou_CEI=False,
                     mes_base_nulo=False,
                     responsavel_nulo=False,
                     contato_nulo=False
                     ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_controle_ponto_nulo(pre_validacao):
        nomeValidacao = "Configuração Rais sem controle de ponto"

        def analisa_controle_ponto_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_controle_ponto_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, i_parametros_rel, sistema_ponto
                                            from bethadba.parametros_rel
                                            where sistema_ponto is null and i_parametros_rel = 2;
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o sistema ponto 2 para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.parametros_rel set sistema_ponto = 2
                                                where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_controle_ponto_nulo: {e}")
            return

        if controle_ponto_nulo:
            dado = analisa_controle_ponto_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_controle_ponto_nulo(listDados=dado)

    def analisa_corrige_inscricao_nulo_ou_CEI(pre_validacao):
        nomeValidacao = "Tipo de inscricao nula ou ser do tipo CEI"

        def analisa_inscricao_nulo_ou_CEI():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_inscricao_nulo_ou_CEI(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, i_parametros_rel, sistema_ponto
                                            from bethadba.parametros_rel
                                            where sistema_ponto is null and i_parametros_rel = 2;
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o tipo de inscricao 'C' para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.parametros_rel set tipo_insc = 'C'
                                                where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_inscricao_nulo_ou_CEI: {e}")
            return

        if inscricao_nulo_ou_CEI:
            dado = analisa_inscricao_nulo_ou_CEI()

            if corrigirErros and len(dado) > 0:
                corrige_inscricao_nulo_ou_CEI(listDados=dado)

    def analisa_corrige_mes_base_nulo(pre_validacao):
        nomeValidacao = "Mês base nulo"

        def analisa_mes_base_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_mes_base_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, i_parametros_rel, mes_base
                                            from bethadba.parametros_rel
                                            where i_parametros_rel = 2 and mes_base is null;
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o tipo de inscricao 'C' para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.parametros_rel set mes_base = 1
                                                where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_mes_base_nulo: {e}")
            return

        if mes_base_nulo:
            dado = analisa_mes_base_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_mes_base_nulo(listDados=dado)

    def analisa_corrige_responsavel_nulo(pre_validacao):
        nomeValidacao = "Nome do responvável nulo"

        def analisa_responsavel_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_responsavel_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, i_parametros_rel, tipo_insc, nome_resp
                                            from bethadba.parametros_rel
                                            where i_parametros_rel = 2 
                                            and nome_resp is null;
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['tipo_insc'] == 'J':
                            buscaNomeEntidade = banco.consultar(f"""SELECT e.apelido from bethadba.entidades e WHERE e.i_entidades = {row['i_entidades']};""")[0]
                            comandoUpdate += f"""UPDATE bethadba.parametros_rel set nome_resp = '{buscaNomeEntidade['apelido']} - {row['i_entidades']}'
                                                 where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""
                            dadoAlterado.append(f"Alterado o nome do responsável para {buscaNomeEntidade['apelido']} - {row['i_entidades']} para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                        elif row['tipo_insc'] == 'F':
                            dadoAlterado.append(f"Alterado o nome do responsável para entidade - {row['i_entidades']} para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")

                            comandoUpdate += f"""UPDATE bethadba.parametros_rel set nome_resp = 'entidade - {row['i_entidades']}'
                                                 where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""

                        else:
                            dadoAlterado.append(f"Alterado o nome do responsável para 'Outro(a) responsável' para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                            comandoUpdate += f"""UPDATE bethadba.parametros_rel set nome_resp = 'Outro(a) responsável'
                                                 where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_responsavel_nulo: {e}")
            return

        if responsavel_nulo:
            dado = analisa_responsavel_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_responsavel_nulo(listDados=dado)

    def analisa_corrige_contato_nulo(pre_validacao):
        nomeValidacao = "Contato nulo"

        def analisa_contato_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_contato_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_entidades, i_parametros_rel, contato, tipo_insc 
                                            from bethadba.parametros_rel
                                            where i_parametros_rel = 2 and tipo_insc = 'J' and contato is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o contato padrão 'contato correção saneamento' para o parametro do relógio {row['i_parametros_rel']} da entidade {row['i_entidades']}")
                        comandoUpdate += f"UPDATE bethadba.parametros_rel set contato = 'contato correção saneamento' where i_entidades = {row['i_entidades']} and i_parametros_rel = {row['i_parametros_rel']};"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_contato_nulo: {e}")
            return

        if contato_nulo:
            dado = analisa_contato_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_contato_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_controle_ponto_nulo(pre_validacao="controle_ponto_nulo")
        analisa_corrige_inscricao_nulo_ou_CEI(pre_validacao="inscricao_nulo_ou_CEI")
        analisa_corrige_mes_base_nulo(pre_validacao='mes_base_nulo')
        analisa_corrige_responsavel_nulo(pre_validacao='responsavel_nulo')
        analisa_corrige_contato_nulo(pre_validacao='contato_nulo')
