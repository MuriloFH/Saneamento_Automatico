from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def motivoAposentadoria(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        motivo_aposentadoria_sem_categoria_esocial=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_motivo_aposentadoria_sem_categoria_esocial(pre_validacao):
        nomeValidacao = "Motivo de Aposentadoria sem categoria eSocial"

        def analisa_motivo_aposentadoria_sem_categoria_esocial():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_motivo_aposentadoria_sem_categoria_esocial(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_motivos_apos,
                        descricao,
                        categoria_esocial
                    FROM 
                        bethadba.motivos_apos
                    WHERE 
                        categoria_esocial IS NULL   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada categoria eSocial do Motivo de Aposentadoria {row['i_motivos_apos']}-{row['descricao']} para 38")
                        comandoUpdate += f"""UPDATE bethadba.motivos_apos set categoria_esocial = '38' where i_motivos_apos = {row['i_motivos_apos']} and categoria_esocial is null;\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_motivo_aposentadoria_sem_categoria_esocial: {e}")
            return

        if motivo_aposentadoria_sem_categoria_esocial:
            dado = analisa_motivo_aposentadoria_sem_categoria_esocial()

            if corrigirErros and len(dado) > 0:
                corrige_motivo_aposentadoria_sem_categoria_esocial(listDados=dado)

    if dadosList:
        analisa_corrige_motivo_aposentadoria_sem_categoria_esocial(pre_validacao="motivo_aposentadoria_sem_categoria_esocial")
