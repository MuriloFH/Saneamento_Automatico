import random
import datetime
from utilitarios.funcoesGenericas.funcoes import adicionaDiasData
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def contratacaoAditivo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       data_aditivo_fora_periodo_vigencia_contrato=False,
                       vigencia_aditivo_nulo=False,
                       vigencia_final_aditivo_diferente_dt_contrato_principal=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_data_aditivo_fora_periodo_vigencia_contrato(pre_validacao):
        nomeValidacao = "Data do aditivo fora do periodo de vigencia do contrato"

        def analisa_data_aditivo_fora_periodo_vigencia_contrato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_aditivo_fora_periodo_vigencia_contrato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:

                for i in listDados:
                    resultado = banco.consultar(f"""SELECT c.i_entidades,
                                                    c.i_contratos,
                                                    isnull(i_contratos_sup_compras,0) as codContratoSup,
                                                    isnull(dateformat(data_ass,'yyyy-mm-dd'),'') as dataAditivo,
                                                    isnull((select c.data_ini_vig from compras.contratos c where c.i_entidades = i_entidades and c.i_contratos = codContratoSup),data_ass) as DataIniVigContratoPrincipal,
                                                    (select max(c.data_vcto) from compras.contratos c where c.i_entidades = i_entidades and (c.i_contratos = codContratoSup or c.i_contratos_sup_compras = codContratoSup) and c.data_ass <= dataAditivo) as DataFinVigContratoPrincipal
                                                    from compras.compras.contratos c 
                                                    where c.i_entidades = {i['i_chave_dsk1']} and c.i_contratos = {i['i_chave_dsk2']}
                                                """)
                    newDataAditivo = 0
                    for row in resultado:
                        if row['DataFinVigContratoPrincipal'] is not None:
                            quantidadeDias = row['DataFinVigContratoPrincipal'] - row['DataIniVigContratoPrincipal']
                            newQuantidadeDias = random.randint(1, quantidadeDias.days)

                            newDataAditivo = adicionaDiasData(data=row['DataIniVigContratoPrincipal'], qtdDias=newQuantidadeDias)
                        else:
                            newDataAditivo = adicionaDiasData(data=row['DataIniVigContratoPrincipal'], qtdDias=30)

                        dadoAlterado.append(f"Alterado a data de assinatura do aditivo {row['i_contratos']} do contrato {row['codContratoSup']} da entidade {row['i_entidades']} para {newDataAditivo}")
                        comandoUpdate += f"""UPDATE compras.compras.contratos set data_ass = '{newDataAditivo}' where i_contratos = {row['i_contratos']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_aditivo_fora_periodo_vigencia_contrato: {e}")
            return

        if data_aditivo_fora_periodo_vigencia_contrato:
            dado = analisa_data_aditivo_fora_periodo_vigencia_contrato()

            if corrigirErros and len(dado) > 0:
                corrige_data_aditivo_fora_periodo_vigencia_contrato(listDados=dado)

    def analisa_corrige_vigencia_aditivo_nulo(pre_validacao):
        nomeValidacao = "Data de vigência do aditivo não informada"

        def analisa_vigencia_aditivo_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_vigencia_aditivo_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            listContratos = []
            listEntidades = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:

                resultado = banco.consultar(f"""SELECT DISTINCT
                                                   i_entidades as chave_dsk1,                 
                                                   i_contratos as chave_dsk2,
                                                   (select first sa.i_sequ_aut from compras.compras.sequ_autor sa where sa.i_contratos = chave_dsk2 and sa.i_entidades = chave_dsk1) as ultimaAf,
                                                   (select first sa.data_aut from compras.compras.sequ_autor sa where sa.i_contratos = chave_dsk2 and sa.i_entidades = chave_dsk1) as dataUltimaAf,
                                                   isnull(i_contratos_sup_compras,0) as codContratoPrincipal,
                                                   natureza,
                                                   ISNULL(dateformat(data_ass,'yyyy-mm-dd'),'') as dataAditivo,
                                                   if natureza in (3,4,7) then ISNULL(dateformat(data_vcto,'yyyy-mm-dd'),'1900-01-01') else '1900-01-01' endif as dataFinalNova,
                                                   dateformat(isnull((select c.data_ini_vig from compras.contratos c where c.i_entidades = chave_dsk1 and c.i_contratos = codContratoPrincipal),data_ass),'yyyy-mm-dd') as DataIniVigContratoPrincipal,
                                                   dateformat((select max(c.data_vcto) from compras.contratos c where c.i_entidades = chave_dsk1 and (c.i_contratos = codContratoPrincipal or c.i_contratos_sup_compras = codContratoPrincipal) and c.data_ass <= dataAditivo),'yyyy-mm-dd') as DataFinVigContratoPrincipal,
                                                   (select date(c.data_vcto) from compras.contratos c where c.i_entidades = chave_dsk1 and c.i_contratos = codContratoPrincipal) as dtcontrato                                                               
                                                FROM  compras.contratos
                                                WHERE  natureza > 1 and natureza is not null 
                                                  and  i_contratos_sup_compras is not null
                                                  and  i_entidades in (1, 2)
                                                  --and chave_dsk2 = 607
                                                  and (dataAditivo = '1900-01-01' or dataFinalNova = '1900-01-01')
                                                  and natureza in (3,4,7)
                                                  order by chave_dsk2, ultimaAf
                                                            """)
                newVigenciaFinal = ""

                for row in resultado:
                    if row['dataFinalNova'] == '1900-01-01':
                        if row['ultimaAf'] is not None:
                            newVigenciaFinal = row['dataUltimaAf']

                        elif row['dataAditivo'] != '' and row['ultimaAf'] is None:
                            newVigenciaFinal = datetime.datetime.strptime(row['dataAditivo'], '%Y-%m-%d')
                            newVigenciaFinal += datetime.timedelta(days=365)
                            newVigenciaFinal = newVigenciaFinal.strftime('%Y-%m-%d')

                        dadoAlterado.append(f"Inserido a data final de vigencia {newVigenciaFinal} para o aditivo do {row['chave_dsk2']} da entidade {row['chave_dsk1']}.")
                        comandoUpdate += f"""UPDATE compras.compras.contratos set data_vcto = '{newVigenciaFinal}' where i_contratos = {row['chave_dsk2']} and i_entidades = {row['chave_dsk1']};\n"""

                    if row['dataAditivo'] == '':  # alteraaaar para ==
                        busca = banco.consultar(f"""select c.i_contratos, c.data_ass, c.data_vcto
                                                    from compras.compras.contratos c
                                                    where c.i_contratos_sup_compras = {row['codContratoPrincipal']} and i_entidades = {row['chave_dsk1']}
                                                    order by c.i_contratos desc, data_ass asc
                                                """)
                        qtdAditivos = len(busca)
                        if qtdAditivos > 0:
                            if qtdAditivos > 1:
                                penultimoAditivo = busca[1]

                                newDataInicioAditivo = penultimoAditivo['data_ass']
                                newDataInicioAditivo += datetime.timedelta(days=1)
                            else:
                                newDataInicioAditivo = datetime.datetime.strptime(row['DataIniVigContratoPrincipal'], '%Y-%m-%d')
                                newDataInicioAditivo += datetime.timedelta(days=1)
                                newDataInicioAditivo = newDataInicioAditivo.strftime('%Y-%m-%d')

                            dadoAlterado.append(f"Inserido a data inicial de vigencia {newDataInicioAditivo} para o aditivo do {row['chave_dsk2']} da entidade {row['chave_dsk1']}.")
                            comandoUpdate += f"""UPDATE compras.compras.contratos set data_ass = '{newDataInicioAditivo}' where i_contratos = {row['chave_dsk2']} and i_entidades = {row['chave_dsk1']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_vigencia_aditivo_nulo: {e}")
            return

        if vigencia_aditivo_nulo:
            dado = analisa_vigencia_aditivo_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_vigencia_aditivo_nulo(listDados=dado)

    def analisa_corrige_vigencia_final_aditivo_diferente_dt_contrato_principal(pre_validacao):
        nomeValidacao = "Vigência final do aditivo diferente da data do contrato principal"

        # para essa função, será alterado somente a natureza do aditivo
        def analisa_vigencia_final_aditivo_diferente_dt_contrato_principal():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_vigencia_final_aditivo_diferente_dt_contrato_principal(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            listContratos = []
            listEntidades = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for row in listDados:
                    dadoAlterado.append(f"""Alterado a natureza do aditivo {row['i_chave_dsk2']} da entidade {row['i_chave_dsk1']} para 4-Aditivo de Prazo e Valor (Acréscimo).""")

                    listContratos.append(str(row['i_chave_dsk2']))
                    listEntidades.append(str(row['i_chave_dsk1']))

                contratos = ','.join(listContratos)
                entidades = ','.join(listEntidades)

                comandoUpdate += f"""UPDATE compras.compras.contratos set natureza = 4 where i_contratos in ({contratos}) and i_entidades in ({entidades});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_vigencia_final_aditivo_diferente_dt_contrato_principal: {e}")
            return

        if vigencia_final_aditivo_diferente_dt_contrato_principal:
            dado = analisa_vigencia_final_aditivo_diferente_dt_contrato_principal()

            if corrigirErros and len(dado) > 0:
                corrige_vigencia_final_aditivo_diferente_dt_contrato_principal(listDados=dado)

    if dadosList:
        analisa_corrige_data_aditivo_fora_periodo_vigencia_contrato(pre_validacao='data_aditivo_fora_periodo_vigencia_contrato')
        analisa_corrige_vigencia_aditivo_nulo(pre_validacao='vigencia_aditivo_nulo')
        analisa_corrige_vigencia_final_aditivo_diferente_dt_contrato_principal(pre_validacao='vigencia_final_aditivo_diferente_dt_contrato_principal')
