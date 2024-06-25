from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def mediaVantagem(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  evento_media_vantagem_composicao_invalida=False,
                  evento_media_vantagem_sem_composicao=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_evento_media_vantagem_composicao_invalida(pre_validacao):
        nomeValidacao = "Eventos de média vantagem estão compondo outros eventos de media vantagem."

        def analisa_evento_media_vantagem_composicao_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_evento_media_vantagem_composicao_invalida(listDados):
            tipoCorrecao = "ALTERAÇÃO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_eventos_medias,
                        i_eventos,
                        (select top 1 i_eventos from bethadba.mediasvant_eve group by i_eventos order by i_eventos desc ) as novo_evento_vinculado
                    FROM
                        bethadba.mediasvant_eve 
                    WHERE 
                        i_eventos IN (SELECT i_eventos FROM bethadba.mediasvant)    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado evento de media vantagem {row['i_eventos_medias']} vinculado a outro evento de media vantagem {row['i_eventos']} para {row['novo_evento_vinculado']}")
                        comandoUpdate += f"""UPDATE bethadba.mediasvant_eve set i_eventos = {row['novo_evento_vinculado']} where i_eventos_medias = {row['i_eventos_medias']} AND i_eventos = {row['i_eventos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_evento_media_vantagem_composicao_invalida: {e}")
            return

        if evento_media_vantagem_composicao_invalida:
            dado = analisa_evento_media_vantagem_composicao_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_evento_media_vantagem_composicao_invalida(listDados=dado)

    def analisa_corrige_evento_media_vantagem_sem_composicao(pre_validacao):
        nomeValidacao = "Eventos de média vantagem sem eventos vinculados."

        def analisa_evento_media_vantagem_sem_composicao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_evento_media_vantagem_sem_composicao(listDados):
            tipoCorrecao = "EXCLUSÃO"
            comandoDelete = ""
            dadoDeletado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        m.i_eventos,                
                        me.i_eventos_medias
                    FROM 
                        bethadba.mediasvant m
                    LEFT JOIN
                        bethadba.mediasvant_eve me ON (m.i_eventos = me.i_eventos_medias)
                    WHERE 
                        me.i_eventos_medias IS NULL   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoDeletado.append(f"Excluido evento {row['i_eventos']} sem vinculo a evento de media vantagem")
                        comandoDelete += f"""DELETE bethadba.mediasvant where i_eventos = {row['i_eventos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoDeletado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_evento_media_vantagem_sem_composicao: {e}")
            return

        if evento_media_vantagem_sem_composicao:
            dado = analisa_evento_media_vantagem_sem_composicao()

            if corrigirErros and len(dado) > 0:
                corrige_evento_media_vantagem_sem_composicao(listDados=dado)

    if dadosList:
        analisa_corrige_evento_media_vantagem_composicao_invalida(pre_validacao="evento_media_vantagem_composicao_invalida")
        analisa_corrige_evento_media_vantagem_sem_composicao(pre_validacao="evento_media_vantagem_sem_composicao")