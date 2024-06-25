from validacoes.funcionarios import validaFuncionario
from validacoes.modeloVeiculos import validaModeloVeiculo
from validacoes.fornecedores import validaFornecedores
from validacoes.motorista import validaMotorista
from validacoes.veiculos import validaVeiculo
from validacoes.veiculoCombustivel import validaVeiculoCombustivel
from validacoes.organogramaVeiculo import validaOrganogramaVeiculo
from validacoes.lancamentoOcorrencia import validaLancamentoOcorrencia
from validacoes.ordemAbastecimento import validaOrdemAbastecimento
from validacoes.lancamentoDespesa import validaLancamentoDespesa
from validacoes.lancamentoDespesaItem import validaLancamentoDespesaItem
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
validaFuncionario.funcionario(tipo_registro='funcionarios', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              cpf_duplicado=True,
                              rg_duplicado=True,
                              cpf_invalido=True,
                              dt_admissao_menor_dt_nascimento=True,
                              dt_nascimento_nulo=True,
                              cpf_nulo=True
                              )

validaModeloVeiculo.modeloVeiculo(tipo_registro='modelo-veiculo', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  marca_desk=True
                                  )

validaFornecedores.fornecedores(tipo_registro='fornecedores', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                cnpj_duplicado=True,
                                cpf_duplicado=True,
                                inscricao_estadual_duplicado=True,
                                cpf_invalido=True,
                                cpf_nulo=True,
                                cnpj_nulo=True
                                )

validaMotorista.motoristas(tipo_registro='motoristas', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                           corrigirErros=True,
                           cpf_nulo=True,
                           primeira_cnh_menor_dt_nascimento=True,
                           dt_emissao_cnh_menor_primeira_cnh=True,
                           dt_emissao_cnh_menor_dt_nascimento=True,
                           cnh_duplicado=True
                           )

validaVeiculo.veiculo(tipo_registro='veiculo-equipamento', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                      corrigirErros=True,
                      renavan_duplicado=True,
                      chassi_duplicado=True,
                      patrimonio_duplicado=True,
                      chassi_maior_20_caracter=True,
                      consumo_maximo_maior_99=True,
                      consumo_minimo_maior_99=True,
                      dt_aquisicao_menor_dt_fabricacao=True,
                      renavam_invalido=True,
                      veiculo_proprio_fornecedor=True,
                      ano_fabricacao_invalido=True
                      )

validaVeiculoCombustivel.veiculoCombustivel(tipo_registro='veiculo-equipamento-combustivel', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                            corrigirErros=True,
                                            veiculo_sem_combustivel_padrao=True,
                                            veiculo_sem_capacidade_volumetrica=True
                                            )

validaOrganogramaVeiculo.veiculoOrganograma(tipo_registro='veiculo-equipamento-organograma', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                            corrigirErros=True,
                                            ano_inicio_veiculo_diferente_ano_centro_custo=True,
                                            dt_inicio_centro_menor_dt_fabricacao_veiculo=True
                                            )

validaLancamentoOcorrencia.lancamentoOcorrencia(tipo_registro='lancamento-ocorrencia', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                                corrigirErros=True,
                                                ocorrencia_motorista_nulo=True
                                                )

validaLancamentoDespesa.lancamentoDespesa(tipo_registro='lancamento-despesa', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                          corrigirErros=True,
                                          despesa_motorista_nulo=True,
                                          caractere_especial_documento=True,
                                          numero_documento_nulo=True,
                                          kilometragem_despesa_inferior_despesa_anterior=True,
                                          mudanca_odometro_sem_lancamento=True
                                          )

validaOrdemAbastecimento.ordemAbastecimento(tipo_registro='ordem-abastecimento-item', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                            corrigirErros=True,
                                            combustivel_ordem_diferente_combustivel_veiculo=True
                                            )

validaLancamentoDespesaItem.lancamentoDespesaItem(tipo_registro='lancamento-despesa-item', logSistema=logTxt, nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes,
                                                  corrigirErros=True,
                                                  valor_unitario_item_despesa_zero=True,
                                                  codigo_item_despesa_nulo=True,
                                                  codigo_material_duplicado=True
                                                  )

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
