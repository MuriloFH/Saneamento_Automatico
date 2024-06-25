from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp
import polars as pl


def responsaveis(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   responsavel_cpf_nulo_ou_invalido=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_responsavel_cpf_nulo_ou_invalido(pre_validacao):
        nomeValidacao = "O responsável não possui CPF informado ou é invalido."

        def analisa_responsavel_cpf_nulo_ou_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)


            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_responsavel_cpf_nulo_ou_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select 
                                            r.i_entidades,
                                            r.i_responsaveis,
                                            r.cpf,
                                            bethadba.dbf_valida_cgc_cpf(null,r.cpf,'F','S') as valida_cpf    
                                        from bethadba.responsaveis r
                                        where 
                                            valida_cpf = 0
                                            or r.cpf is null 
                                            or r.cpf = ''
                                        order by r.i_responsaveis     
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        newCpf = geraCfp()
                        dadoAlterado.append(f"Alterado CPF do responsável {row['i_responsaveis']}  de {row['cpf']} para {newCpf}")
                        comandoUpdate += f"""UPDATE bethadba.responsaveis set cpf = '{newCpf}' where i_responsaveis = {row['i_responsaveis']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_responsavel_cpf_nulo_ou_invalido: {e}")
            return

        if responsavel_cpf_nulo_ou_invalido:
            dado = analisa_responsavel_cpf_nulo_ou_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_responsavel_cpf_nulo_ou_invalido(listDados=dado)


    if dadosList:
        analisa_corrige_responsavel_cpf_nulo_ou_invalido(pre_validacao="responsavel_cpf_nulo_ou_invalido")





