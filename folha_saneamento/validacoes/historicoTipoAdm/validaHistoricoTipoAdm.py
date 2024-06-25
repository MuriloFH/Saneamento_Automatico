from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta



def historicoTipoAdm(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 tipo_adm_com_teto_remuneratorio_nulo=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_tipo_adm_com_teto_remuneratorio_nulo(pre_validacao):
        nomeValidacao = "Histórico de tipo de administração com teto remuneratório nulo."

        def analisa_tipo_adm_com_teto_remuneratorio_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_tipo_adm_com_teto_remuneratorio_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_tipos_adm,
                        i_competencias,
                        coalesce((SELECT TOP 1 vlr_sub_teto FROM bethadba.hist_tipos_adm where vlr_sub_teto is not null GROUP BY vlr_sub_teto ORDER BY COUNT(*) DESC),20000) as novo_teto
                    from bethadba.hist_tipos_adm 
                    where vlr_sub_teto is null   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado teto remuneratório do histórico de tipo de administração {row['i_tipos_adm']} competência {row['i_competencias']} para {row['novo_teto']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_tipos_adm set vlr_sub_teto = {row['novo_teto']} where i_tipos_adm = {row['i_tipos_adm']} and i_competencias = '{row['i_competencias']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_tipo_adm_com_teto_remuneratorio_nulo: {e}")
            return

        if tipo_adm_com_teto_remuneratorio_nulo:
            dado = analisa_tipo_adm_com_teto_remuneratorio_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_tipo_adm_com_teto_remuneratorio_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_tipo_adm_com_teto_remuneratorio_nulo(pre_validacao="tipo_adm_com_teto_remuneratorio_nulo")
