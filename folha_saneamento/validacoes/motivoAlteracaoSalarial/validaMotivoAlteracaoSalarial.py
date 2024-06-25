from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def motivoAlteracaoSalarial(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                            corrigirErros=False,
                            descricao_duplicada=False
                            ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_descricao_duplicada(pre_validacao):
        nomeValidacao = "Descrição de motivo de alteração salarial duplicado."

        def analisa_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        LIST(i_motivos_altsal ) as motivoss, 
                        TRIM(descricao) as descric, 
                        COUNT(descricao) AS quantidade
                    FROM 
                        bethadba.motivos_altsal 
                    GROUP BY 
                        descric
                    HAVING 
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_motivo = row['motivoss'].split(',')
                        for motivo in list_motivo[1:]:
                            novo_nome = row['descric'] + ' ' + motivo
                            if len(novo_nome) <= 40:
                                dadoAlterado.append(f"Alterada descrição do motivo de alteração salarial {motivo} de {row['descric']} para {novo_nome}")
                                comandoUpdate += f"""UPDATE bethadba.motivos_altsal set descricao = '{novo_nome}' where i_motivos_altsal = {motivo};\n"""
                            else:
                                novo_nome = row['descric'][:40 - len(motivo)] + motivo
                                dadoAlterado.append(f"Alterada descrição do motivo de alteração salarial {motivo} de {row['descric']} para {novo_nome}")
                                comandoUpdate += f"""UPDATE bethadba.motivos_altsal set descricao = '{novo_nome}' where i_motivos_altsal = {motivo};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_duplicada: {e}")
            return

        if descricao_duplicada:
            dado = analisa_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_duplicada(listDados=dado)

    if dadosList:
        analisa_corrige_descricao_duplicada(pre_validacao="descricao_duplicada")
