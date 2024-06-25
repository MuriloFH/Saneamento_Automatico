from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def motivoRescisao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   motivo_rescisao_sem_categoria_esocial=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_motivo_rescisao_sem_categoria_esocial(pre_validacao):
        nomeValidacao = "Motivo de Rescisão sem categoria eSocial"

        def analisa_motivo_rescisao_sem_categoria_esocial():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_motivo_rescisao_sem_categoria_esocial(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_motivos_resc,
                        descricao,
                        categoria_esocial
                    FROM 
                        bethadba.motivos_resc
                    WHERE 
                        categoria_esocial IS NULL and dispensados not in (3)   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada categoria eSocial do Motivo de Rescisao {row['i_motivos_resc']}-{row['descricao']} para 02")
                        comandoUpdate += f"""UPDATE bethadba.motivos_resc set categoria_esocial = '02' where i_motivos_resc = {row['i_motivos_resc']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_motivo_rescisao_sem_categoria_esocial: {e}")
            return

        if motivo_rescisao_sem_categoria_esocial:
            dado = analisa_motivo_rescisao_sem_categoria_esocial()

            if corrigirErros and len(dado) > 0:
                corrige_motivo_rescisao_sem_categoria_esocial(listDados=dado)

    if dadosList:
        analisa_corrige_motivo_rescisao_sem_categoria_esocial(pre_validacao="motivo_rescisao_sem_categoria_esocial")
