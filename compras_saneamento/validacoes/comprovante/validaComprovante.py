from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl

def comprovante(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       liquidacao_af_documento_zerado=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_liquidacao_af_documento_zerado(pre_validacao):
        nomeValidacao = "Liquidação da AF possuí documento fiscal com valor zero."

        def analisa_liquidacao_af_documento_zerado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_liquidacao_af_documento_zerado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" 
                    SELECT 
                        dctos_fiscais.i_ano_aut,
                        dctos_fiscais.i_entidades, 
                        dctos_fiscais.i_sequ_aut,
                        dctos_fiscais.i_sequ_liq, 
                        dctos_fiscais.i_numero_dcto_fiscal,
                        dctos_fiscais.i_entidades as entidades,
                        dctos_fiscais.valor_dcto_fiscal as valorBruto,
                        liquidacao.valor_bruto_nota  
                    FROM compras.dctos_fiscais 
                    JOIN compras.liquidacao ON (dctos_fiscais.i_entidades = liquidacao.i_entidades AND
                                                    dctos_fiscais.i_ano_aut = liquidacao.i_ano_aut AND
                                                    dctos_fiscais.i_sequ_aut = liquidacao.i_sequ_aut AND
                                                    dctos_fiscais.i_sequ_liq = liquidacao.i_sequ_liq) join compras.sequ_autor seq
                    WHERE 
                        i_numero_dcto_fiscal IS NOT NULL 
                        and	seq.i_simples is null
                        and valor_dcto_fiscal = 0
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado valor da  Liquidação {row['i_sequ_liq']} da AF {row['i_sequ_aut']}/{row['i_ano_aut']} de 0 para {row['valor_bruto_nota']}")
                        comandoUpdate += f"""UPDATE compras.dctos_fiscais set valor_dcto_fiscal = {row['valor_bruto_nota']} where i_sequ_aut = {row['i_sequ_aut']} and i_ano_aut = {row['i_ano_aut']} and i_sequ_liq = {row['i_sequ_liq']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_liquidacao_af_documento_zerado: {e}")
            return

        if liquidacao_af_documento_zerado:
            dado = analisa_liquidacao_af_documento_zerado()

            if corrigirErros and len(dado) > 0:
                corrige_liquidacao_af_documento_zerado(listDados=dado)

    if dadosList:
        analisa_corrige_liquidacao_af_documento_zerado(pre_validacao="liquidacao_af_documento_zerado")
