from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def eventos(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
            corrigirErros=False,
            evento_sem_historico=False
            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_evento_sem_historico(pre_validacao):
        nomeValidacao = "Evento sem histórico"

        def analisa_evento_sem_historico():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_evento_sem_historico(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            SELECT 
                                            i_eventos, nome, tipo_pd, taxa, unidade, sai_rais, compoe_liq, compoe_hmes, digitou_form, classif_evento, desativado, inverte_compoe_liq,
                                            caracteristica, grupo_provisao, deduc_fundo_financ, envia_fly_transparencia, montar_base_fgts_integral_afast, codigo_tce, classif_provisao
                                            from bethadba.eventos
                                            where eventos.i_eventos not in (select i_eventos from bethadba.hist_eventos);
                                        """)

                for row in dados:
                    competencia = datetime.datetime.now()
                    ano = competencia.year
                    mes = competencia.month

                    competencia = datetime.datetime(year=ano, month=mes, day=1).date()

                    dadoAlterado.append(f"Inserido o histórico para o evento {row['i_eventos']}")
                    comandoInsert += f"""insert into bethadba.hist_eventos (i_eventos, i_competencias, nome, tipo_pd, taxa, unidade, sai_rais, compoe_liq, compoe_hmes, digitou_form, classif_evento, desativado, inverte_compoe_liq, 
                                            caracteristica, grupo_provisao, deduc_fundo_financ, envia_fly_transparencia, montar_base_fgts_integral_afast, codigo_tce, classif_provisao)
                                            VALUES({row['i_eventos']}, '{competencia}', '{row['nome']}', '{row['tipo_pd']}', {row['taxa']}, '{row['unidade']}', '{row['sai_rais']}', '{row['compoe_liq']}', '{row['compoe_hmes']}','{row['digitou_form']}',
                                                    {row['classif_evento']}, '{row['desativado']}', '{row['inverte_compoe_liq']}', '{row['caracteristica']}', {row['grupo_provisao']}, '{row['deduc_fundo_financ']}', '{row['envia_fly_transparencia']}',
                                                    '{row['montar_base_fgts_integral_afast']}', {row['codigo_tce']}, {row['classif_provisao']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_evento_sem_historico: {e}")
            return

        if evento_sem_historico:
            dado = analisa_evento_sem_historico()

            if corrigirErros and len(dado) > 0:
                corrige_evento_sem_historico(listDados=dado)

    if dadosList:
        analisa_corrige_evento_sem_historico(pre_validacao="evento_sem_historico")
