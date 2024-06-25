from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import colorama


def beneficiario(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 beneficiario_nao_cadastrano_na_tabela=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_beneficiario_nao_cadastrano_na_tabela(pre_validacao):
        nomeValidacao = "Funcionário com campo adicional indicando ser beneficiario porém não esta cadastrado como beneficiario"

        def analisa_beneficiario_nao_cadastrano_na_tabela():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_beneficiario_nao_cadastrano_na_tabela(listDados):
            tipoCorrecao = "INSERSAO"
            comandoInsert = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select f.i_pessoas,fpa.i_entidades, fpa.i_funcionarios, valor_texto, i_caracteristicas, valor_caracter
                                            from bethadba.funcionarios_prop_adic fpa  
                                            join bethadba.funcionarios f on (f.i_entidades = fpa.i_entidades and f.i_funcionarios = fpa.i_funcionarios)
                                             where fpa.i_caracteristicas  = 20369 
                                             and fpa.valor_caracter = '2'
                                             and fpa.i_funcionarios not in (select b.i_funcionarios from bethadba.beneficiarios b) 
                                             order by f.i_funcionarios
                                        """)
                if len(busca) > 0:
                    for row in busca:
                        dependente = row['i_pessoas']

                        buscaInstituidor = banco.consultar(f"""SELECT f.i_funcionarios, f.i_entidades, d.*
                                                                from bethadba.dependentes d
                                                                join bethadba.funcionarios f on (f.i_pessoas = d.i_pessoas and f.i_entidades = 1)
                                                                where d.i_dependentes = {dependente}
                                                            """)
                        if len(buscaInstituidor) > 0:
                            buscaInstituidor = buscaInstituidor[0]

                            dadoAlterado.append(f"Inserido a matricula {row['i_funcionarios']} como beneficiário(a) na entidade {row['i_entidades']}")
                            comandoInsert += f"""insert into bethadba.beneficiarios (i_entidades, i_funcionarios, i_entidades_inst, i_instituidor, duracao_ben, perc_recebto, config, parecer_interno, situacao, matricula_instituidor)
                                                    values({row['i_entidades']}, {row['i_funcionarios']}, {buscaInstituidor['i_entidades']}, {buscaInstituidor['i_funcionarios']}, 'V', 30, 1, 'F', 3, {buscaInstituidor['i_funcionarios']});\n
                                            """
                        else:
                            print(colorama.Fore.RED, f"Não localizado nenhum dado na tabela de dependentes para a pessoa {dependente} na entidade {row['i_entidades']}, favor analisar manualmente o caso.")

                banco.executarComLog(comando=banco.triggerOff(comandoInsert, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_beneficiario_nao_cadastrano_na_tabela: {e}")
            return

        if beneficiario_nao_cadastrano_na_tabela:
            dado = analisa_beneficiario_nao_cadastrano_na_tabela()

            if corrigirErros and len(dado) > 0:
                corrige_beneficiario_nao_cadastrano_na_tabela(listDados=dado)

    if dadosList:
        analisa_corrige_beneficiario_nao_cadastrano_na_tabela(pre_validacao="beneficiario_nao_cadastrano_na_tabela")
