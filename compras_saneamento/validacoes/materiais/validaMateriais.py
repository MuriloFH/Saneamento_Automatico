from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def materiais(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
              corrigirErros=False,
              materiais_tipo_servicos_nao_estocavel=False
              ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_materiais_tipo_servicos_nao_estocavel(pre_validacao):
        nomeValidacao = "Materiais com tipo SERVIÇO, não pode ser ESTOCÁVEL"

        def analisa_materiais_tipo_servicos_nao_estocavel():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_materiais_tipo_servicos_nao_estocavel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(""" select 
                                            m.i_material, 
                                            m.nome_mat,
                                            m.mat_serv, 
                                            m.estocavel, 
                                            m.i_entidades 
                                        from compras.material m
                                        where m.mat_serv = 'S' 
                                        and m.estocavel = 'S'    
                                         """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)

                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Alterado material {row['i_material']} - {row['nome_mat']} para não estocável")
                        comandoUpdate += f"""update compras.material set estocavel = 'N' where i_material = {row['i_material']} and estocavel = 'S' and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_materiais_tipo_servicos_nao_estocavel: {e}")
            return

        if materiais_tipo_servicos_nao_estocavel:
            dado = analisa_materiais_tipo_servicos_nao_estocavel()

            if corrigirErros and len(dado) > 0:
                corrige_materiais_tipo_servicos_nao_estocavel(listDados=dado)

    if dadosList:
        analisa_corrige_materiais_tipo_servicos_nao_estocavel(pre_validacao="materiais_tipo_servicos_nao_estocavel")
