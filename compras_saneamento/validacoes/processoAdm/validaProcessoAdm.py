from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraNumeroLicitacao, validaDtInicioRecebimentoMaiorDtFimRecebimento
import datetime


def processoAdm(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                licitacao_duplicada=False,
                ano_contratacao_menor_ano_processo=False,
                data_hora_abertura_licitacao_menor_inicio_recebimento=False,
                data_hora_abertura_licitacao_menor_final_recebimento=False,
                data_inicio_recebimento_menor_data_processo=False,
                data_inicio_recebimento_menor_data_final_recebimento=False,
                data_expiracao_comissao_menor_data_processo=False,
                processo_ata_sem_data_homologacao=False,
                processo_sem_participante_com_data_homologacao=False,
                processo_homologado_adjudicado_sem_ata=False,
                fundamento_processo_com_registro_preco_em_modalidade_especifica=False,
                tipo_processo_nao_permite_registro_preco_informado=False,
                nome_orgao_processo_maior_60_caracter=False,
                processo_homologado_sem_ata=False,
                modalidade_processo_outras_com_tipo_objeto_diferente_5=False,
                saldo_pendente_entre_entidades=False,
                quantidade_item_superior_licitada=False,
                incisao_nulo_processos_tipo_pregao=False,
                dispensa_pregao_com_inciso_incorreto=False,
                registro_preco_nulo_com_contrato_marcado_ata_registro_preco=False,
                processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_licitacao_duplicada(pre_validacao):
        nomeValidacao = "Numero da licitação duplicado"

        def analisa_licitacao_duplicada():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_licitacao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT COUNT(licitacao), list(i_ano_proc) as ano, list(i_processo) as processo, list(modalidade) as mod,list(licitacao) as licit, list(i_entidades) as entidade
                                            from compras.compras.processos
                                            group by i_ano_proc, i_entidades, modalidade, licitacao
                                            HAVING COUNT(licitacao) > 1""")

                if len(busca) > 0:
                    for row in busca:
                        ano = row['ano'].split(',')
                        processo = row['processo'].split(',')
                        modalidade = row['mod'].split(',')
                        entidade = row['entidade'].split(',')
                        licitacao = row['licit'].split(',')
                        for indice in range(0, len(processo)):
                            newLicitacao = geraNumeroLicitacao(licitacao[indice])
                            comandoUpdate += f"""UPDATE compras.compras.processos set licitacao = {newLicitacao} where i_ano_proc = {ano[indice]} and i_processo = {processo[indice]} and modalidade = {modalidade[indice]} and i_entidades = {entidade[indice]};\n"""
                            dadoAlterado.append(f"Alterado o numero da licitação para {newLicitacao} do processo {processo[indice]}/{ano[indice]}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_compra_nulo: {e}")
            return

        if licitacao_duplicada:
            dado = analisa_licitacao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_licitacao_duplicada(listDados=dado)

    def analisa_corrige_ano_contratacao_menor_ano_processo(pre_validacao):
        nomeValidacao = "Ano de contratação menor que ano do processo administrativo"

        def analisa_ano_contratacao_menor_ano_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ano_contratacao_menor_ano_processo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT ano_licit, i_ano_proc, i_processo, modalidade, i_entidades
                                            from compras.compras.processos p
                                            where p.ano_licit < p.i_ano_proc
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.processos set ano_licit = {row['i_ano_proc']} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado ano da contratação para {row['i_ano_proc']} do processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ano_contratacao_menor_ano_processo: {e}")
            return

        if ano_contratacao_menor_ano_processo:
            dado = analisa_ano_contratacao_menor_ano_processo()

            if corrigirErros and len(dado) > 0:
                corrige_ano_contratacao_menor_ano_processo(listDados=dado)

    def analisa_corrige_data_hora_abertura_licitacao_menor_inicio_recebimento(pre_validacao):
        nomeValidacao = "Data/hora da abertura da licitação menor ou igual a data/hora de inicio do recebimento"

        def analisa_data_hora_abertura_licitacao_menor_inicio_recebimento():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_hora_abertura_licitacao_menor_inicio_recebimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT dateformat(data_abertura + hora_abertura, 'yyyy-mm-dd hh:mm:ss') as abertura,
                                            dateformat(data_entrega + hora_entrega, 'yyyy-mm-dd hh:mm:ss') as recebimento,
                                            data_abertura,
                                            hora_abertura,
                                            data_entrega,
                                            hora_entrega,
                                            i_ano_proc,
                                            i_processo,
                                            modalidade,
                                            i_entidades
                                            from compras.compras.processos p
                                            where abertura <= recebimento
                                            and abertura is not null 
                                            and recebimento is not null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDataAbertura = row['data_entrega']
                        newHoraAbertura = row['hora_entrega']
                        newHoraAbertura = (datetime.datetime.combine(datetime.datetime.min, newHoraAbertura) + datetime.timedelta(hours=1)).time()

                        comandoUpdate += f"""UPDATE compras.compras.processos set data_abertura = '{newDataAbertura}' , hora_abertura = '{newHoraAbertura}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a data e hora do processo {row['i_processo']}/{row['i_ano_proc']} para {newDataAbertura} {newHoraAbertura}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_hora_abertura_licitacao_menor_inicio_recebimento: {e}")
            return

        if data_hora_abertura_licitacao_menor_inicio_recebimento:
            dado = analisa_data_hora_abertura_licitacao_menor_inicio_recebimento()

            if corrigirErros and len(dado) > 0:
                corrige_data_hora_abertura_licitacao_menor_inicio_recebimento(listDados=dado)

    def analisa_corrige_data_hora_abertura_licitacao_menor_final_recebimento(pre_validacao):
        nomeValidacao = "Data/hora da abertura da licitação menor ou igual a data/hora de final do recebimento"

        def analisa_data_hora_abertura_licitacao_menor_final_recebimento():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_hora_abertura_licitacao_menor_final_recebimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                if len(listDados) > 0:
                    for i in listDados:
                        busca = banco.consultar(f"""SELECT dateformat(data_abertura + hora_abertura, 'yyyy-mm-dd hh:mm:ss') as abertura,
                                                    dateformat(data_recebimento + hora_recebimento, 'yyyy-mm-dd hh:mm:ss') as recebimentoFinal,
                                                    data_abertura,
                                                    hora_abertura,
                                                    data_recebimento,
                                                    hora_recebimento,
                                                    i_ano_proc,
                                                    i_processo,
                                                    modalidade,
                                                    i_entidades
                                                    from compras.compras.processos p
                                                    where abertura <= recebimentoFinal
                                                    and p.i_ano_proc = {i['i_chave_dsk2']}
                                                    and p.i_processo = {i['i_chave_dsk3']}
                                                """)
                        if len(busca) > 0:
                            for row in busca:
                                newDataRecebimento = row['data_abertura']
                                newHoraRecebimento = row['hora_abertura']
                                newHoraRecebimento = (datetime.datetime.combine(datetime.datetime.min, newHoraRecebimento) - datetime.timedelta(hours=1)).time()

                                comandoUpdate += f"""UPDATE compras.compras.processos set data_recebimento = '{newDataRecebimento}' , hora_recebimento = '{newHoraRecebimento}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                                dadoAlterado.append(f"Alterado a data e hora do processo {row['i_processo']}/{row['i_ano_proc']} para {newDataRecebimento} {newHoraRecebimento}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)
                validaDtInicioRecebimentoMaiorDtFimRecebimento(banco)
                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_hora_abertura_licitacao_menor_final_recebimento: {e}")
            return

        if data_hora_abertura_licitacao_menor_final_recebimento:
            dado = analisa_data_hora_abertura_licitacao_menor_final_recebimento()

            if corrigirErros and len(dado) > 0:
                corrige_data_hora_abertura_licitacao_menor_final_recebimento(listDados=dado)

    def analisa_corrige_data_inicio_recebimento_menor_data_processo(pre_validacao):
        nomeValidacao = "Data de inicio do recebimento do envelope menor que data do processo"

        def analisa_data_inicio_recebimento_menor_data_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_inicio_recebimento_menor_data_processo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT data_processo as dataProcesso,
                                            data_entrega as dataEntrega,
                                            i_ano_proc,
                                            i_processo,
                                            modalidade,
                                            i_entidades
                                            from compras.compras.processos p
                                            where dataEntrega < dataProcesso
                                            and dataEntrega is not null
                                            and dataProcesso is not null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDataEntrega = row['dataProcesso']
                        comandoUpdate += f"""UPDATE compras.compras.processos set data_entrega = '{newDataEntrega}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a data de entrega dos envelopes do processo {row['i_processo']}/{row['i_ano_proc']} para {newDataEntrega}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_inicio_recebimento_menor_data_processo: {e}")
            return

        if data_inicio_recebimento_menor_data_processo:
            dado = analisa_data_inicio_recebimento_menor_data_processo()

            if corrigirErros and len(dado) > 0:
                corrige_data_inicio_recebimento_menor_data_processo(listDados=dado)

    def analisa_corrige_data_inicio_recebimento_menor_data_final_recebimento(pre_validacao):
        nomeValidacao = "Data de inicio do recebimento do envelope menor que data final de recebimento"

        def analisa_data_inicio_recebimento_menor_data_final_recebimento():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_inicio_recebimento_menor_data_final_recebimento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT isnull(dateformat(p.data_entrega + p.hora_entrega, 'yyyy-mm-dd hh:mm:ss'), '') as dataInicioRecebimentoEnvelope,
                                            isnull(dateformat(p.data_recebimento + p.hora_recebimento, 'yyyy-mm-dd hh:mm:ss'), '') as dataFinalRecebimentoEnvelope,
                                            p.i_ano_proc, i_processo, modalidade, i_entidades, data_entrega, hora_entrega
                                            from compras.compras.processos p 
                                            where dataFinalRecebimentoEnvelope < dataInicioRecebimentoEnvelope
                                            and dataFinalRecebimentoEnvelope != ''
                                            and dataInicioRecebimentoEnvelope != ''
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDataRecebimentoEnvelope = row['data_entrega']
                        newHoraRecebimentoEnvelope = (datetime.datetime.combine(datetime.datetime.min, row['hora_entrega']) + datetime.timedelta(minutes=1)).time()

                        comandoUpdate += f"""UPDATE compras.compras.processos p set p.data_recebimento = '{newDataRecebimentoEnvelope}', hora_recebimento = '{newHoraRecebimentoEnvelope}' where p.i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a hora do recebimento do envelope do processo {row['i_processo']}/{row['i_ano_proc']} para {newHoraRecebimentoEnvelope}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_inicio_recebimento_menor_data_final_recebimento: {e}")
            return

        if data_inicio_recebimento_menor_data_final_recebimento:
            dado = analisa_data_inicio_recebimento_menor_data_final_recebimento()

            if corrigirErros and len(dado) > 0:
                corrige_data_inicio_recebimento_menor_data_final_recebimento(listDados=dado)

    def analisa_corrige_data_expiracao_comissao_menor_data_processo(pre_validacao):
        nomeValidacao = "Data de expiracao da comissao menor que data do processo"

        def analisa_data_expiracao_comissao_menor_data_processo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_expiracao_comissao_menor_data_processo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT 
                                            isnull((select dateformat(res.data_expira, 'yyyy-mm-dd')
                                                    from compras.responsaveis res
                                                    where res.i_entidades = processos.i_entidades and res.i_responsavel = processos.i_responsavel),
                                            (select max(dateformat(pro.data_processo,'yyyy-mm-dd'))
                                                from compras.processos pro
                                                where pro.i_entidades = processos.i_entidades and pro.i_responsavel = processos.i_responsavel and pro.i_ano_proc = processos.i_ano_proc),''  
                                            ) as dataExpiraComissao,
                                            processos.data_processo as dtProcesso,
                                            (select dateformat(res.data_expira, 'yyyy-mm-dd') 
                                                from compras.responsaveis res 
                                                where res.i_entidades = processos.i_entidades and res.i_responsavel = processos.i_responsavel) as dataExpira,
                                            (select max(dateformat(pro.data_processo,'yyyy-mm-dd'))
                                                from compras.processos pro 
                                                where pro.i_entidades = processos.i_entidades and pro.i_responsavel = processos.i_responsavel and pro.i_ano_proc = processos.i_ano_proc) as dataExpira2,
                                            processos.i_ano_proc,
                                            processos.i_processo,
                                            processos.modalidade,
                                            processos.i_entidades
                                            from compras.compras.processos
                                            inner join compras.compras.responsaveis r on (r.i_responsavel = processos.i_responsavel)
                                            where dataExpiraComissao < dtProcesso
                                            and dataExpiraComissao <> ''
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDataProcesso = row['dataExpiraComissao']
                        comandoUpdate += f"""UPDATE compras.compras.processos set data_processo = '{newDataProcesso}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a data do processo {row['i_processo']}/{row['i_ano_proc']} para {newDataProcesso}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_expiracao_comissao_menor_data_processo: {e}")
            return

        if data_expiracao_comissao_menor_data_processo:
            dado = analisa_data_expiracao_comissao_menor_data_processo()

            if corrigirErros and len(dado) > 0:
                corrige_data_expiracao_comissao_menor_data_processo(listDados=dado)

    def analisa_corrige_processo_ata_sem_data_homologacao(pre_validacao):
        nomeValidacao = "Processo com Ata informada mas sem data de homologação"

        def analisa_processo_ata_sem_data_homologacao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_ata_sem_data_homologacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, i_processo, i_coleta, modalidade,i_entidades, data_processo, data_publicacao, data_homolog
                                            from compras.compras.processos p 
                                            where p.i_coleta is not null and data_homolog is null;
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newDataHomologacao = row['data_publicacao']
                        newDataHomologacao += datetime.timedelta(days=30)

                        comandoUpdate += f"""UPDATE compras.compras.processos set data_homolog = '{newDataHomologacao}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a data de homologação do processo {row['i_processo']}/{row['i_ano_proc']} para {newDataHomologacao}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_ata_sem_data_homologacao: {e}")
            return

        if processo_ata_sem_data_homologacao:
            dado = analisa_processo_ata_sem_data_homologacao()

            if corrigirErros and len(dado) > 0:
                corrige_processo_ata_sem_data_homologacao(listDados=dado)

    def analisa_corrige_processo_sem_participante_com_data_homologacao(pre_validacao):
        nomeValidacao = "Processo sem participante com data de homologação"

        def analisa_processo_sem_participante_com_data_homologacao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_sem_participante_com_data_homologacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, i_processo, modalidade, i_entidades, i_coleta, data_homolog, temParticipante = isnull((Select first 1 from compras.participantes pa where pa.i_entidades = processos.i_entidades and pa.i_processo = processos.i_processo and pa.i_ano_proc = processos.i_ano_proc order by 1),0)
                                            from compras.compras.processos
                                            WHERE temParticipante = 0 and data_homolog is not null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.processos set data_homolog = null where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado a data de homologação para vazio do processo {row['i_processo']}/{row['i_ano_proc']} ")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_inicio_recebimento_menor_data_processo: {e}")
            return

        if processo_sem_participante_com_data_homologacao:
            dado = analisa_processo_sem_participante_com_data_homologacao()

            if corrigirErros and len(dado) > 0:
                corrige_processo_sem_participante_com_data_homologacao(listDados=dado)

    def analisa_corrige_processo_homologado_adjudicado_sem_ata(pre_validacao):
        nomeValidacao = "Processo com data de homologação e adjudicado sem ata registrada"

        def analisa_processo_homologado_adjudicado_sem_ata():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_homologado_adjudicado_sem_ata(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            anoProcAtual = None
            entidadeAtual = None
            iColeta = 0
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, i_processo, modalidade, i_entidades, data_processo, reg_prec, temAdjuticacao = isnull((SELECT FIRST hp.i_credores from compras.homologa_parcial hp, compras.adjudicacao adj where (hp.i_ano_proc = p.i_ano_proc and hp.i_processo = p.i_processo) or (adj.i_ano_proc = p.i_ano_proc and adj.i_processo = p.i_processo)),0)
                                            from compras.compras.processos p
                                            where reg_prec = 'S' and i_coleta is null and data_homolog is not null and temAdjuticacao != 0
                                            order by p.i_entidades
                                        """)

                lastResponsavel = banco.consultar(f"SELECT min(r.i_responsavel) as responsavel from compras.compras.responsaveis r")[0]['responsavel']

                if len(busca) > 0:
                    for row in busca:

                        if anoProcAtual != row['i_ano_proc'] or entidadeAtual != row['i_entidades']:
                            anoProcAtual = row['i_ano_proc']
                            entidadeAtual = row['i_entidades']

                            lastColeta = banco.consultar(f"""SELECT max(i_coleta) as ultimaColeta from compras.compras.coletas c where c.i_ano_coleta = {row['i_ano_proc']} and c.i_entidades = {row['i_entidades']}""")
                            if lastColeta[0]['ultimaColeta'] is not None:
                                iColeta = int(lastColeta[0]['ultimaColeta']) + 1
                            else:
                                iColeta = 1
                        else:
                            iColeta += 1

                        comandoInsert += f"""INSERT INTO compras.compras.coletas(i_ano_coleta, i_coleta, i_responsavel, data_coleta, i_entidades, i_ano_proc, i_processo)
                                                values({row['i_ano_proc']}, {iColeta}, {lastResponsavel}, '{row['data_processo']}', {row['i_entidades']}, {row['i_ano_proc']}, {row['i_processo']}); \n"""

                        comandoUpdate += f"""UPDATE compras.compras.processos set i_coleta = {iColeta} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Inserido a ata {iColeta} no processo {row['i_processo']}/{row['i_ano_proc']} ")

                banco.executar(banco.triggerOff(comandoInsert))
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_homologado_sem_ata: {e}")
            return

        if processo_homologado_adjudicado_sem_ata:
            dado = analisa_processo_homologado_adjudicado_sem_ata()

            if corrigirErros and len(dado) > 0:
                corrige_processo_homologado_adjudicado_sem_ata(listDados=dado)

    def analisa_corrige_fundamento_processo_com_registro_preco_em_modalidade_especifica(pre_validacao):
        nomeValidacao = "Fundamento legal com registro de preço para modalidades 8, 9 e 11."

        def analisa_fundamento_processo_com_registro_preco_em_modalidade_especifica():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_fundamento_processo_com_registro_preco_em_modalidade_especifica(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, 
                                            i_processo, 
                                            modalidade, 
                                            i_entidades, 
                                            IF(isnull(processos.inciso,'') in ('CAPUT','§ UNICO','§1','§1 I','§1 II','§1 III','§1 IV','§1 V','§1 VI','§2','§3','§4','Art.14 § 1','Art.17  II','Art.17 I','Art.17 I(a)','Art.17 I(b)','Art.17 I(c)','Art.17 I(d)','Art.17 I(e)','Art.17 I(f)','Art.17 I(g)','Art.17 I(h)','Art.17 I(i)','Art.17 II(a)','Art.17 II(b)','Art.17 II(c)','Art.17 II(d)','Art.17 II(e)','Art.17 II(f)','IX','V','VI','VII','VIII','X','XI','XII','XIII','XIV','XIX','XV','XVI','XVII','XVIII','XX','XXI','XXII','XXIII','XXIV','XXIX','XXV','XXVI','XXVII','XXVIII','XXX','XXXI','XXXII','XXXIII','XXXIV')) THEN
                                                                '8.666/93'
                                                             ELSE IF(isnull(processos.inciso,'') in ('I','II','III','IV','V')) THEN
                                                                    IF(processos.modalidade = 15) THEN '12.462/2011' ELSE '8.666/93' ENDIF
                                                                  ELSE IF(isnull(processos.inciso,'') = '§3') THEN
                                                                         '12.462/2011'
                                                              ELSE IF(isnull(processos.inciso,'') = 'Art.14 CAPUT') THEN
                                                                   '11.947/2009'
                                                               ELSE IF(isnull(processos.inciso,'') in ('Art.30 I','Art.30 II','Art.30 III','Art.30 VI','Art.31 CAPUT','L13019 Art.24','L13019 Art.31 I','L13019 Art.31 II')) THEN
                                                                    '13.019/2014'
                                                                ELSE IF(isnull(processos.inciso,'') in ('L13303 Art.28 I','L13303 Art.28 II','L13303 Art.29 I','L13303 Art.29 II','L13303 Art.29 III','L13303 Art.29 IV','L13303 Art.29 IX','L13303 Art.29 V','L13303 Art.29 VI','L13303 Art.29 VII','L13303 Art.29 VIII','L13303 Art.29 X','L13303 Art.29 XI','L13303 Art.29 XII','L13303 Art.29 XIII','L13303 Art.29 XIV','L13303 Art.29 XV','L13303 Art.29','XVI','L13303 Art.29 XVII','L13303 Art.29 XVIII','L13303 Art.30 I','L13303 Art.30 IIA','L13303 Art.30 IIB','L13303 Art.30 IIC','L13303 Art.30 IID','L13303 Art.30 IIE','L13303 Art.30 IIF','L13303 Art.30 IIG')) THEN
                                                                     '13.303/2016'
                                                                 ELSE IF(isnull(processos.inciso,'') in ('Art.4 CAPUT','Art.4 G')) THEN
                                                                          '13.979/2020'
                                                                 ELSE IF (isnull(processos.inciso,'') in ('L14.133/21 Art.4° CAPUT','L14.133/21 Art.4º §2°','L14.133/21 Art.17 §2°','L14.133/21 Art.23 §1° V','L14.133/21 Art.17 §3°','L14.133/21 Art.17 §4°','L14.133/21 Art.17 §5°','L14.133/21 Art.17 §6°','L14.133/21 Art.23 §1° I',
                                                                                                        'L14.133/21 Art.23 §1° II','L14.133/21 Art.23 §1° III','L14.133/21 Art.23 §1° IV','L14.133/21 Art.23 §2° I','L14.133/21 Art.23 §2° II','L14.133/21 Art.23 §2° III','L14.133/21 Art.23 §2° IV','L14.133/21 Art.23 §3°',
                                                                                                        'L14.133/21 Art.26 II','L14.133/21 Art.23 §4°','L14.133/21 Art.23 §5°','L14.133/21 Art.24 I','L14.133/21 Art.26 I','L14.133/21 Art.29 CAPUT','L14.133/21 Art.30','L14.133/21 Art.31 CAPUT','L14.133/21 Art.32 I.a',
                                                                                                        'L14.133/21 Art.32 I.b','L14.133/21 Art.32 I.c','L14.133/21 Art.32 II.a','L14.133/21 Art.32 II.b','L14.133/21 Art.32 II.c','L14.133/21 Art.35','L14.133/21 Art.47 I','L14.133/21 Art.47 II','L14.133/21 Art.74 IV',
                                                                                                        'L14.133/21 Art.47 §2°','L14.133/21 Art.49','L14.133/21 Art.72','L14.133/21 Art.74 I','L14.133/21 Art.74 II','L14.133/21 Art.74 III.a','L14.133/21 Art.74 III.b','L14.133/21 Art.74 III.c','L14.133/21 Art.74 III.d',
                                                                                                        'L14.133/21 Art.74 III.e','L14.133/21 Art.74 III.f','L14.133/21 Art.74 III.g','L14.133/21 Art.74 III.h','L14.133/21 Art.74 V','L14.133/21 Art.76 I.a','L14.133/21 Art.76 I.b','L14.133/21 Art.76 I.c','L14.133/21 Art.76 I.d',
                                                                                                        'L14.133/21 Art.76 I.e','L14.133/21 Art.76 I.f','L14.133/21 Art.76 I.g','L14.133/21 Art.76 I.h','L14.133/21 Art.76 I.i','L14.133/21 Art.76 I.j','L14.133/21 Art.76 II.a','L14.133/21 Art.76 II.b','L14.133/21 Art.76 II.c',
                                                                                                        'L14.133/21 Art.76 II.d','L14.133/21 Art.76 II.e','L14.133/21 Art.76 II.f','L14.133/21 Art.76 §1°','L14.133/21 Art.76 §2°','L14.133/21 Art.76 §3°I','L14.133/21 Art.76 §3°II','L14.133/21 Art.79.I','L14.133/21 Art.79.II',
                                                                                                        'L14.133/21 Art.79.III','L14.133/21 Art.82 §6°','L14.133/21 Art.83','L14.133/21 Art.84','L14.133/21 Art.85.I','L14.133/21 Art.85.II','L14.133/21 Art.86 §7°','L14.133/21 Art.75 I','L14.133/21 Art.75 II','L14.133/21 Art.75 III.a',
                                                                                                        'L14.133/21 Art.75 III.b','L14.133/21 Art.75 IV.a','L14.133/21 Art.75 IV.b','L14.133/21 Art.75 IV.c','L14.133/21 Art.75 IV.d','L14.133/21 Art.75 IV.e','L14.133/21 Art.75 IV.f','L14.133/21 Art.75 IV.g','L14.133/21 Art.75 IV.h',
                                                                                                        'L14.133/21 Art.75 IV.i','L14.133/21 Art.75 IV.j','L14.133/21 Art.75 IV.k','L14.133/21 Art.75 IV.l','L14.133/21 Art.75 IV.m','L14.133/21 Art.75 V','L14.133/21 Art.75 VI','L14.133/21 Art.75 VII','L14.133/21 Art.75 VIII',
                                                                                                        'L14.133/21 Art.75 IX','L14.133/21 Art.75 X','L14.133/21 Art.75 XI','L14.133/21 Art.75 XII','L14.133/21 Art.75 XIII','L14.133/21 Art.75 XIV','L14.133/21 Art.75 XV','L14.133/21 Art.75 XVI','L14.133/21 Art.75 §2°',
                                                                                                        'L14.133/21 Art.75 §5°','L14.133/21 Art.75 §6°','L14.133/21 Art.75 §7°','L14.133/21 Art.74 §3°','L14.133/21 Art.28 I','L14.133/21 Art.6 XLI','L14.133/21 Art.78 IV','L14.133/21 Art.28 II','L14.133/21 Art.6 XXXVIII')) THEN
                                                                            '14.133/2021'
                                                                      ELSE '8.666/93'
                                                             ENDIF ENDIF ENDIF ENDIF ENDIF ENDIF ENDIF ENDIF as lei,
                                            reg_prec,
                                            i_coleta,
                                            registroPreco = if processos.reg_prec = 'S' then
                                                                'true' 
                                                            else 
                                                                if processos.i_coleta is not null then
                                                                    'true'
                                                                else 
                                                                    'false'
                                                                endif
                                                            endif
                                            from compras.compras.processos
                                            where modalidade in (8, 9, 11) and lei != '14.133/2021' and registroPreco = 'true'
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['i_coleta'] is not None:
                            comandoUpdate += f"""UPDATE compras.compras.processos set reg_prec = 'N', i_coleta = null where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        else:
                            comandoUpdate += f"""UPDATE compras.compras.processos set reg_prec = 'N' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""

                        dadoAlterado.append(f"Removido o registro de preço do processo {row['i_processo']}/{row['i_ano_proc']} ")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fundamento_processo_com_registro_preco_em_modalidade_especifica: {e}")
            return

        if fundamento_processo_com_registro_preco_em_modalidade_especifica:
            dado = analisa_fundamento_processo_com_registro_preco_em_modalidade_especifica()

            if corrigirErros and len(dado) > 0:
                corrige_fundamento_processo_com_registro_preco_em_modalidade_especifica(listDados=dado)

    def analisa_corrige_tipo_processo_nao_permite_registro_preco_informado(pre_validacao):
        nomeValidacao = "Tipo de processo 13 não é permitido registro de preço"

        def analisa_tipo_processo_nao_permite_registro_preco_informado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_processo_nao_permite_registro_preco_informado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, 
                                            i_processo, 
                                            modalidade, 
                                            i_entidades,
                                            tipo_objeto
                                            from compras.compras.processos p 
                                            where reg_prec = 'S' and tipo_objeto = 13
                                        """)

                if len(busca) > 0:
                    for row in busca:

                        comandoUpdate += f"""UPDATE compras.compras.processos set tipo_objeto = 1 where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado o tipo de objeto do processo {row['i_processo']}/{row['i_ano_proc']} para 1 Mat. e Serv.")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_processo_nao_permite_registro_preco_informado: {e}")
            return

        if tipo_processo_nao_permite_registro_preco_informado:
            dado = analisa_tipo_processo_nao_permite_registro_preco_informado()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_processo_nao_permite_registro_preco_informado(listDados=dado)

    def analisa_corrige_nome_orgao_processo_maior_60_caracter(pre_validacao):
        nomeValidacao = "Nome do orgão gerenciador maior que 60 caracter"

        def analisa_nome_orgao_processo_maior_60_caracter():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_nome_orgao_processo_maior_60_caracter(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc, 
                                            i_processo, 
                                            modalidade, 
                                            i_entidades,
                                            tipo_objeto
                                            from compras.compras.processos p 
                                            where reg_prec = 'S' and tipo_objeto = 13
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.processos set tipo_objeto = 1 where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado o tipo de objeto do processo {row['i_processo']}/{row['i_ano_proc']} para 1 Mat. e Serv.")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_nome_orgao_processo_maior_60_caracter: {e}")
            return

        if nome_orgao_processo_maior_60_caracter:
            dado = analisa_nome_orgao_processo_maior_60_caracter()

            if corrigirErros and len(dado) > 0:
                corrige_nome_orgao_processo_maior_60_caracter(listDados=dado)

    def analisa_corrige_processo_homologado_sem_ata(pre_validacao):
        nomeValidacao = "Processo homologado sem ata"

        def analisa_processo_homologado_sem_ata():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_homologado_sem_ata(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select i_processo, i_ano_proc, i_entidades, modalidade, data_processo, isnull(i_coleta, 0)
                                        from
                                            compras.processos
                                        where
                                            i_ano_proc >= 1900
                                            and reg_prec = 'S'
                                            and i_coleta is null
                                            and i_ano_coleta is null
                                            and i_entidades = i_entidades
                                            and data_homolog is not null
                                        """)

                lastResponsavel = banco.consultar(f"SELECT min(r.i_responsavel) as responsavel from compras.compras.responsaveis r")[0]['responsavel']

                if len(busca) > 0:
                    for row in busca:
                        lastColeta = banco.consultar(f"""SELECT max(i_coleta) as ultimaColeta from compras.compras.coletas c where c.i_ano_coleta = {row['i_ano_proc']}""")
                        if lastColeta[0]['ultimaColeta'] is not None:
                            iColeta = int(lastColeta[0]['ultimaColeta']) + 1
                        else:
                            iColeta = 1

                        comandoInsert += f"""INSERT INTO compras.compras.coletas(i_ano_coleta, i_coleta, i_responsavel, data_coleta, i_entidades, i_ano_proc, i_processo)
                                                                values({row['i_ano_proc']}, {iColeta}, {lastResponsavel}, '{row['data_processo']}', {row['i_entidades']}, {row['i_ano_proc']}, {row['i_processo']}); \n"""

                        comandoUpdate += f"""UPDATE compras.compras.processos set i_coleta = {iColeta} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Inserido a ata {iColeta} no processo {row['i_processo']}/{row['i_ano_proc']} ")

                banco.executar(banco.triggerOff(comandoInsert))
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_homologado_sem_ata: {e}")
            return

        if processo_homologado_sem_ata:
            dado = analisa_processo_homologado_sem_ata()

            if corrigirErros and len(dado) > 0:
                corrige_processo_homologado_sem_ata(listDados=dado)

    def analisa_corrige_modalidade_processo_outras_com_tipo_objeto_diferente_5(pre_validacao):
        nomeValidacao = "Processo com modalidade 'Outras' e tipo de objeto diferente de 5"

        def analisa_modalidade_processo_outras_com_tipo_objeto_diferente_5():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_modalidade_processo_outras_com_tipo_objeto_diferente_5(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select i_processo, i_ano_proc, modalidade, i_entidades
                                            from
                                                compras.processos 
                                            where 
                                                modalidade = 99
                                                and tipo_objeto <> 5
                                                and i_ano_proc >= 1900
                                                and i_entidades = i_entidades
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.processos set tipo_objeto = 5 where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Alterado o tipo do objeto do processo {row['i_processo']}/{row['i_ano_proc']} para 1 - Mat. e Serv")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_modalidade_processo_outras_com_tipo_objeto_diferente_5: {e}")
            return

        if modalidade_processo_outras_com_tipo_objeto_diferente_5:
            dado = analisa_modalidade_processo_outras_com_tipo_objeto_diferente_5()

            if corrigirErros and len(dado) > 0:
                corrige_modalidade_processo_outras_com_tipo_objeto_diferente_5(listDados=dado)

    def analisa_corrige_saldo_pendente_entre_entidades(pre_validacao):
        nomeValidacao = "Divisão de saldo pendente entre entidades de um processo"

        def analisa_saldo_pendente_entre_entidades():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_saldo_pendente_entre_entidades(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select  
                                            ent.i_ano_proc,
                                            ent.i_processo,
                                            ent.i_item,
                                            ite.qtde_item,
                                            sum(ent.qtd_estimada) as qtd_estimada,
                                            ite.qtde_item - sum(ent.qtd_estimada) as faltaDividir,
                                            ite.i_entidades
                                        from compras.itens_processo ite
                                            left join compras.processos_entidades_partic_itens ent 
                                            on (ite.i_ano_proc  = ent.i_ano_proc
                                            and ite.i_processo  = ent.i_processo
                                            and ite.i_item      = ent.i_item
                                            and ite.i_entidades = ent.i_entidades)
                                        where ent.i_ano_proc in (
                                            select i_ano_proc 
                                            from compras.processos_entidades_partic_itens)
                                        and ent.i_processo in (
                                            select i_processo 
                                            from compras.processos_entidades_partic_itens)
                                        group by ite.i_entidades,ent.i_ano_proc, ent.i_processo,ent.i_item,ite.qtde_item
                                        having (faltaDividir) > '0.00'
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.itens_processo set qtde_item = {row['qtd_estimada']} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_item = {row['i_item']};\n"""
                        dadoAlterado.append(f"Ajustado a quantidade de itens para {row['qtd_estimada']} do processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_saldo_pendente_entre_entidades: {e}")
            return

        if saldo_pendente_entre_entidades:
            dado = analisa_saldo_pendente_entre_entidades()

            if corrigirErros and len(dado) > 0:
                corrige_saldo_pendente_entre_entidades(listDados=dado)


    def analisa_corrige_quantidade_item_superior_licitada(pre_validacao):
        nomeValidacao = "Quantidade do item superior ao licitado"

        def analisa_quantidade_item_superior_licitada():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_quantidade_item_superior_licitada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select
                                            ent.i_ano_proc,
                                            ent.i_processo,
                                            ent.i_item,
                                            sum(ent.qtd_estimada) as qtd_estimada,
                                            ite.qtde_item,
                                            sum(ent.qtd_estimada) - ite.qtde_item as qtdUltrapassada,
                                            ent.i_entidades
                                         from compras.itens_processo ite
                                            left join compras.processos_entidades_partic_itens ent
                                            on (ite.i_ano_proc = ent.i_ano_proc
                                            and ite.i_processo = ent.i_processo
                                            and ite.i_item = ent.i_item
                                            and ite.i_entidades = ent.i_entidades)
                                          where ent.i_ano_proc in (select i_ano_proc
                                            from compras.processos_entidades_partic_itens)
                                            and ent.i_processo in (select i_processo
                                            from compras.processos_entidades_partic_itens)
                                            and ent.i_entidades = ent.i_entidades
                                            and ent.i_ano_proc >= 1900
                                            group by ite.i_entidades,ent.i_ano_proc, ent.i_processo, ent.i_item, ite.qtde_item, ent.i_entidades
                                            having (qtdUltrapassada) > '0.00'
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        comandoUpdate += f"""UPDATE compras.compras.itens_processo set qtde_item = {row['qtd_estimada']} where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and i_item = {row['i_item']};\n"""
                        dadoAlterado.append(f"Ajustado a quantidade de itens para {row['qtd_estimada']} do processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_quantidade_item_superior_licitada: {e}")
            return

        if quantidade_item_superior_licitada:
            dado = analisa_quantidade_item_superior_licitada()

            if corrigirErros and len(dado) > 0:
                corrige_quantidade_item_superior_licitada(listDados=dado)

    def analisa_corrige_incisao_nulo_processos_tipo_pregao(pre_validacao):
        nomeValidacao = "Processos do tipo pregão com inciso nulo"

        def analisa_incisao_nulo_processos_tipo_pregao():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_incisao_nulo_processos_tipo_pregao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select i_processo,
                                            i_ano_proc,
                                            i_entidades,
                                            modalidade,
                                            inciso
                                            from compras.processos 
                                            where i_entidades = i_entidades
                                            and i_ano_proc >= 1900
                                            and modalidade in (13,14)
                                            and inciso is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['i_ano_proc'] <= 2023:
                            newInciso = 'XXI'
                        else:
                            newInciso = 'L14.133/21 ART.28 I'

                        comandoUpdate += f"""UPDATE compras.compras.processos set inciso = '{newInciso}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Definido o inciso {newInciso} para o processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_incisao_nulo_processos_tipo_pregao: {e}")
            return

        if incisao_nulo_processos_tipo_pregao:
            dado = analisa_incisao_nulo_processos_tipo_pregao()

            if corrigirErros and len(dado) > 0:
                corrige_incisao_nulo_processos_tipo_pregao(listDados=dado)

    def analisa_corrige_dispensa_pregao_com_inciso_incorreto(pre_validacao):
        nomeValidacao = "Processos do tipo dispensa e pregão com inciso incorreto"

        def analisa_dispensa_pregao_com_inciso_incorreto():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dispensa_pregao_com_inciso_incorreto(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""SELECT i_ano_proc,
                                                i_processo,
                                                modalidade,
                                                inciso,
                                                i_entidades
                                            FROM compras.processos
                                            WHERE(
                                             (modalidade IN (13, 14) AND inciso in ('I', 'II')) -- Modalidade Pregao com fundamento I ou II
                                             OR
                                             (modalidade IN (8) AND inciso = 'CAPUT') -- Modalidade Dispensa de licitacao com fundamento CAPUT
                                             )
                                            AND i_ano_proc >= 1900
                                            AND i_entidades = i_entidades
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        if row['i_ano_proc'] <= 2023 and row['modalidade'] in (13, 14):
                            newInciso = 'XXI'
                        elif row['i_ano_proc'] >= 2024 and row['modalidade'] in (13, 14):
                            newInciso = 'L14.133/21 ART.28 I'
                        else:
                            newInciso = 'II'

                        comandoUpdate += f"""UPDATE compras.compras.processos set inciso = '{newInciso}' where i_ano_proc = {row['i_ano_proc']} and i_processo = {row['i_processo']} and modalidade = {row['modalidade']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Definido o inciso {newInciso} para o processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dispensa_pregao_com_inciso_incorreto: {e}")
            return

        if dispensa_pregao_com_inciso_incorreto:
            dado = analisa_dispensa_pregao_com_inciso_incorreto()

            if corrigirErros and len(dado) > 0:
                corrige_dispensa_pregao_com_inciso_incorreto(listDados=dado)

    def analisa_corrige_registro_preco_nulo_com_contrato_marcado_ata_registro_preco(pre_validacao):
        nomeValidacao = "Processo com a flag registro de preço desmarcada porem com contrato marcado como Ata registro de preço"

        def analisa_registro_preco_nulo_com_contrato_marcado_ata_registro_preco():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_registro_preco_nulo_com_contrato_marcado_ata_registro_preco(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select distinct pro.i_processo,
                                                            pro.i_ano_proc,
                                                            pro.data_processo,
                                                            pro.modalidade,
                                                            pro.i_entidades,
                                                            pro.i_coleta,
                                                            pro.i_responsavel,
                                                            con.i_contratos,
                                                            con.ata_reg_prec,
                                                            pro.reg_prec
                                            from compras.contratos con inner join compras.processos pro on (con.i_processo = pro.i_processo and con.i_ano_proc = pro.i_ano_proc and con.i_entidades = pro.i_entidades) 
                                            where pro.i_ano_proc >= 1900
                                                  and pro.i_entidades = pro.i_entidades
                                                  and con.natureza = 1
                                                  and con.ata_reg_prec = 'S'
                                                  and pro.reg_prec = 'N'
                                                  and pro.modalidade in (5,6,8,11,13,14)
                                                  and pro.data_homolog is not null
                                                  //and con.i_contratos = 377
                                            order by pro.i_ano_proc
                                        """)
                lastResponsavel = banco.consultar(f"SELECT min(r.i_responsavel) as responsavel from compras.compras.responsaveis r")[0]['responsavel']

                if len(busca) > 0:
                    for row in busca:
                        if row['i_coleta'] is None:
                            lastColeta = banco.consultar(f"""SELECT max(i_coleta) as ultimaColeta from compras.compras.coletas c where c.i_ano_coleta = {row['i_ano_proc']}""")[0]['ultimaColeta']
                            if lastColeta is not None:
                                iColeta = int(lastColeta) + 1
                            else:
                                iColeta = 1

                            comandoInsert += f"""INSERT INTO compras.compras.coletas(i_ano_coleta, i_coleta, i_responsavel, data_coleta, i_entidades, i_ano_proc, i_processo)
                                                    values({row['i_ano_proc']}, {iColeta}, {lastResponsavel}, '{row['data_processo']}', {row['i_entidades']}, {row['i_ano_proc']}, {row['i_processo']});\n"""
                            dadoAlterado.append(f"Inserido a coleta {iColeta} para o processo {row['i_processo']}/{row['i_ano_proc']}")

                        comandoUpdate += f"""update compras.compras.processos c set reg_prec = 'S' where c.i_ano_proc = {row['i_ano_proc']} and c.i_processo = {row['i_processo']} and i_entidades = {row['i_entidades']};\n"""
                        dadoAlterado.append(f"Definido a flag Reg. de Preço como 'S' para o processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executar(comando=banco.triggerOff(comandoInsert))
                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_registro_preco_nulo_com_contrato_marcado_ata_registro_preco: {e}")
            return

        if registro_preco_nulo_com_contrato_marcado_ata_registro_preco:
            dado = analisa_registro_preco_nulo_com_contrato_marcado_ata_registro_preco()

            if corrigirErros and len(dado) > 0:
                corrige_registro_preco_nulo_com_contrato_marcado_ata_registro_preco(listDados=dado)

    def analisa_corrige_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo(pre_validacao):
        nomeValidacao = "Processo com movimentações com a coluna data_contrato_transformers nula"

        def analisa_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select c.i_processo, c.i_ano_proc, c.i_entidades, list(p.modalidade) as mod, list(c.data_vcto) dataVencimento
                                            from compras.processos p key join compras.contratos c
                                            where c.i_entidades = p.i_entidades 
                                            and p.data_contrato_transformers is null
                                            group by c.i_processo, c.i_ano_proc, c.i_entidades 
                                            order by c.i_ano_proc
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        modalidade = row['mod'].split(',')[0]
                        newData = sorted(row['dataVencimento'].split(','), reverse=True)[0]

                        comandoUpdate += f"""UPDATE compras.compras.processos p set p.data_contrato_transformers = '{newData}' where p.i_processo = {row['i_processo']} and p.i_ano_proc = {row['i_ano_proc']} and p.i_entidades = {row['i_entidades']} and p.modalidade = {modalidade};\n"""
                        dadoAlterado.append(f"Inserido a data {newData} na coluna data_contrato_transformers do processo {row['i_processo']}/{row['i_ano_proc']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo: {e}")
            return

        if processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo:
            dado = analisa_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_licitacao_duplicada(pre_validacao="licitacao_duplicada")
        analisa_corrige_ano_contratacao_menor_ano_processo(pre_validacao='ano_contratacao_menor_ano_processo')
        analisa_corrige_data_hora_abertura_licitacao_menor_inicio_recebimento(pre_validacao='data_hora_abertura_licitacao_menor_inicio_recebimento')
        analisa_corrige_data_hora_abertura_licitacao_menor_final_recebimento(pre_validacao='data_hora_abertura_licitacao_menor_final_recebimento')
        analisa_corrige_data_inicio_recebimento_menor_data_processo(pre_validacao='data_inicio_recebimento_menor_data_processo')
        analisa_corrige_data_inicio_recebimento_menor_data_final_recebimento(pre_validacao='data_inicio_recebimento_menor_data_final_recebimento')
        analisa_corrige_data_expiracao_comissao_menor_data_processo(pre_validacao='data_expiracao_comissao_menor_data_processo')
        analisa_corrige_processo_ata_sem_data_homologacao(pre_validacao='processo_ata_sem_data_homologacao')
        analisa_corrige_processo_sem_participante_com_data_homologacao(pre_validacao='processo_sem_participante_com_data_homologacao')
        analisa_corrige_processo_homologado_adjudicado_sem_ata(pre_validacao='processo_homologado_adjudicado_sem_ata')
        analisa_corrige_fundamento_processo_com_registro_preco_em_modalidade_especifica(pre_validacao='fundamento_processo_com_registro_preco_em_modalidade_especifica')
        analisa_corrige_tipo_processo_nao_permite_registro_preco_informado(pre_validacao='tipo_processo_nao_permite_registro_preco_informado')
        analisa_corrige_nome_orgao_processo_maior_60_caracter(pre_validacao='nome_orgao_processo_maior_60_caracter')
        analisa_corrige_processo_homologado_sem_ata(pre_validacao='processo_homologado_sem_ata')
        analisa_corrige_modalidade_processo_outras_com_tipo_objeto_diferente_5(pre_validacao='modalidade_processo_outras_com_tipo_objeto_diferente_5')
        analisa_corrige_saldo_pendente_entre_entidades(pre_validacao='saldo_pendente_entre_entidades')
        analisa_corrige_quantidade_item_superior_licitada(pre_validacao='quantidade_item_superior_licitada')
        analisa_corrige_incisao_nulo_processos_tipo_pregao(pre_validacao='incisao_nulo_processos_tipo_pregao')
        analisa_corrige_dispensa_pregao_com_inciso_incorreto(pre_validacao='dispensa_pregao_com_inciso_incorreto')
        analisa_corrige_registro_preco_nulo_com_contrato_marcado_ata_registro_preco(pre_validacao='registro_preco_nulo_com_contrato_marcado_ata_registro_preco')
        analisa_corrige_processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo(pre_validacao='processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo')
