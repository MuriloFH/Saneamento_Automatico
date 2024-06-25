from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def aposentado(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
               corrigirErros=False,
               dt_hist_posterior_dt_cessacao=False,
               aposentado_sem_rescisao=False
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_dt_hist_posterior_dt_cessacao(pre_validacao):
        nomeValidacao = "Data de alteração do historico maior que a data de cessacao da aposentadoria"

        def analisa_dt_hist_posterior_dt_cessacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_hist_posterior_dt_cessacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""select distinct
                                                funcionarios.i_entidades as i_entidades,
                                                funcionarios.i_funcionarios as i_funcionarios,
                                                dataAlteracao = hist_funcionarios.dt_alteracoes,
                                                inicioVigencia = hist_funcionarios.dt_alteracoes,
                                                dataCessacaoAposentadoria = isnull(rescisoes.dt_canc_resc,(select resc.dt_rescisao
                                                                                                         from bethadba.rescisoes resc join
                                                                                                              bethadba.motivos_resc mot on (resc.i_motivos_resc = mot.i_motivos_resc)
                                                                                                         where resc.i_entidades = funcionarios.i_entidades
                                                                                                           and resc.i_funcionarios = funcionarios.i_funcionarios
                                                                                                           and mot.dispensados = 4                                
                                                                                                           and resc.dt_canc_resc is null
                                                                                                       )),
                                                situacao = if dataCessacaoAposentadoria is not null then 'CESSADO' else 'APOSENTADO' endif                                                                    
                                            from 
                                                bethadba.funcionarios ,
                                                bethadba.hist_funcionarios , 
                                                bethadba.rescisoes , 
                                                bethadba.motivos_resc , 
                                                bethadba.motivos_apos ,
                                                bethadba.tipos_afast
                                            where 
                                                funcionarios.i_entidades = rescisoes.i_entidades and 
                                                funcionarios.i_funcionarios = rescisoes.i_funcionarios and 
                                                funcionarios.i_entidades = hist_funcionarios.i_entidades and
                                                funcionarios.i_funcionarios = hist_funcionarios.i_funcionarios and 
                                                hist_funcionarios.dt_alteracoes = bethadba.dbf_getdatahisfun(funcionarios.i_entidades, funcionarios.i_funcionarios, getdate() ) and 
                                                rescisoes.i_motivos_resc = motivos_resc.i_motivos_resc and 
                                                rescisoes.i_motivos_apos = motivos_apos.i_motivos_apos and 
                                                motivos_apos.i_tipos_afast = tipos_afast.i_tipos_afast and 
                                                tipos_afast.classif = 9  
                                                and dataCessacaoAposentadoria < dataalteracao
                                            order by i_entidades, i_funcionarios
                                        """)

                for row in dados:
                    newDtAlteracao = row['dataCessacaoAposentadoria']
                    newDtAlteracao -= datetime.timedelta(days=1)

                    dadoAlterado.append(f"""Alterado a data de alteração para {newDtAlteracao} do historico do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}""")
                    comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set dt_alteracoes = '{newDtAlteracao}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dataAlteracao']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_hist_posterior_dt_cessacao: {e}")
            return

        if dt_hist_posterior_dt_cessacao:
            dado = analisa_dt_hist_posterior_dt_cessacao()

            if corrigirErros and len(dado) > 0:
                corrige_dt_hist_posterior_dt_cessacao(listDados=dado)

    def analisa_corrige_aposentado_sem_rescisao(pre_validacao):
        nomeValidacao = "Aposentado sem rescisão"

        def analisa_aposentado_sem_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_aposentado_sem_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select
                                            fpa.i_entidades, fpa.i_funcionarios
                                            from bethadba.funcionarios_prop_adic fpa
                                            where fpa.i_caracteristicas  = 20369 
                                                  and fpa.valor_caracter = '1'
                                                  and fpa.i_funcionarios not in (select r.i_funcionarios from bethadba.rescisoes r
                                                            where r.i_entidades = fpa.i_entidades and r.i_motivos_apos is not null and r.dt_canc_resc is null)
                                        """)

                for row in dados:
                    entidade = row['i_entidades']
                    funcionario = row['i_funcionarios']

                    lastCompFolha = banco.consultar(f"""SELECT first dc.i_competencias
                                                        from bethadba.dados_calc dc
                                                        where dc.i_entidades = {entidade} and i_funcionarios = {funcionario}
                                                        order by dc.i_competencias desc
                                                    """)

                    if len(lastCompFolha) > 0:
                        dtRescisao = lastCompFolha[0]['i_competencias']
                        dtAviso = dtRescisao
                    else:
                        dtRescisao = datetime.date.today()
                        dtAviso = dtRescisao

                    dadoAlterado.append(f"Inserido a rescisão para o funcionário {funcionario} da entidade {entidade}")
                    comandoInsert += f"""INSERT INTO bethadba.rescisoes(i_entidades, i_funcionarios, i_rescisoes, i_motivos_resc, dt_rescisao, aviso_ind, dt_aviso, vlr_saldo_fgts, fgts_mesant, compl_mensal, trab_dia_resc, aviso_desc)
                                            values({entidade}, {funcionario}, 1, 7, '{dtRescisao}', 'N', '{dtAviso}', 0, 'S', 'N', 'N', 'N');\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_aposentado_sem_rescisao: {e}")
            return

        if aposentado_sem_rescisao:
            dado = analisa_aposentado_sem_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_aposentado_sem_rescisao(listDados=dado)

    if dadosList:
        analisa_corrige_dt_hist_posterior_dt_cessacao(pre_validacao="dt_hist_posterior_dt_cessacao")
        analisa_corrige_aposentado_sem_rescisao(pre_validacao="aposentado_sem_rescisao")
