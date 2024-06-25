from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def rescisao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             rescisoes_aposentadoria_motivo_nulo=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_rescisoes_aposentadoria_motivo_nulo(pre_validacao):
        nomeValidacao = "Rescisões de Aposentadoria com motivo nulo."

        def analisa_rescisoes_aposentadoria_motivo_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_rescisoes_aposentadoria_motivo_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_entidades,
                        i_funcionarios,
                        i_rescisoes,
                        i_motivos_apos
                    FROM 
                        bethadba.rescisoes 
                    WHERE 
                        i_motivos_resc = 7 
                        AND i_motivos_apos IS NULL   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado motivo de aposentadoria da rescisao {row['i_rescisoes']} do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} de nulo para 1")
                        comandoUpdate += f"""UPDATE bethadba.rescisoes set i_motivos_apos = 1 where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_rescisoes = {row['i_rescisoes']} and i_motivos_resc = 7 ;\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_rescisoes_aposentadoria_motivo_nulo: {e}")
            return

        if rescisoes_aposentadoria_motivo_nulo:
            dado = analisa_rescisoes_aposentadoria_motivo_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_rescisoes_aposentadoria_motivo_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_rescisoes_aposentadoria_motivo_nulo(pre_validacao="rescisoes_aposentadoria_motivo_nulo")
