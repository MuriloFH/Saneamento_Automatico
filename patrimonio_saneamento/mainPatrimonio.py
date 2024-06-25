from validacoes.organograma import validaOrganograma
from validacoes.localizacaoFisica import validaLocalizacaoFisica
from validacoes.responsaveis import validaResponsaveis
from validacoes.fornecedor import validaFornecedores
from validacoes.bens import validaBens
from validacoes.baixas import validaBaixas
from validacoes.manutencaoBem import validaManutencaoBem
from utilitarios.bancoDeAlteracoes.logAlteracoes import LogAlteracoes
from utilitarios.logExecucao.funcoesLogExecucao import LogTxt
import datetime
from utilitarios.popularTabelaControle.popularTabelaControle import populaTabela

logTxt = LogTxt()

nomeOdbc = ''

# ------------Inicia Banco de logs------------
logAlteracoes = LogAlteracoes(host="localhost",
                              dataBase="logsAlteracoes",
                              usuario="postgres",
                              senha="postgres")

logAlteracoes.criaTabela()
# --------------------------------------------

# ------------Popula a tabela de controle-----
populaTabela(odbc=nomeOdbc, sistema='Frotas')
# --------------------------------------------

timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
# ------------Inicia Pré-validações------------

validaOrganograma.organograma(tipo_registro='organograma', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              descricao_unidade_maior_60_caracter=True
                              )

validaLocalizacaoFisica.localizacaoFisica(tipo_registro='localizacao-fisica', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                          corrigirErros=True,
                                          descricao_duplicada=True
                                          )

validaResponsaveis.responsaveis(tipo_registro='responsavel', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                cpf_duplicado=True,
                                cod_siafe_nulo=True,
                                cpf_nulo=True,
                                cpf_invalido=True
                                )

validaFornecedores.fornecedores(tipo_registro='fornecedores', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                fornecedor_cidade_nula=True,
                                cnpj_cpf_nulo=True,
                                cnpj_cpf_duplicado=True
                                )

validaBens.bens(tipo_registro='bem', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                valor_aquisicao_nulo=True,
                valor_depreciado_maior_valor_aquisicao=True,
                valor_depreciado_maior_valor_depreciavel=True,
                dt_depreciacao_menor_dt_aquisicao_bem=True,
                bem_sem_responsavel=True,
                descricao_bem_maior_1024_caracter=True,
                tempo_garantia_negativo=True,
                vida_util_maior_zero_com_depreciacao_anual_igual_zero=True,
                valor_residual_superior_liquido_contabil=True,
                numero_placa_nulo=True,
                placa_duplicada=True
                )

validaBaixas.baixas(tipo_registro='baixa', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                    corrigirErros=True,
                    historico_nulo=True,
                    motivo_nulo=True,
                    bem_placa_nulo=True,
                    data_baixa_superior_data_aquisicao_bem=True,
                    numero_boletim_ocorrencia_maior_oito_caracteres=True,
                    numero_processo_maior_oito_caracteres=True,
                    numero_processo_caracter_especial=True,
                    numero_boletim_ocorrencia_caracter_especial=True
                    )

validaManutencaoBem.manutencaoBem(tipo_registro='manutencao-bem', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  codigo_bem_nulo=True,
                                  data_envio_manutencao_nulo=True,
                                  prestador_servico_nulo=True
                                  )

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
