from validacoes.unidadesMedida import validaUnidadesMedida
from validacoes.materiais import validaMateriais
from validacoes.lotesMateriais import validaLotesMateriais
from validacoes.fornecedor import validaFornecedor
from validacoes.responsaveis import validaResponsaveis
from validacoes.organograma import validaOrganograma
from validacoes.entradasItens import validaEntradasItens
from validacoes.saidasItens import validaSaidasItens
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
populaTabela(odbc=nomeOdbc, sistema='Almoxarifado')
# --------------------------------------------

timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
# ------------Inicia Pré-validações------------

validaUnidadesMedida.unidadesMedida(tipo_registro='unidades-medida', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    unidade_medida_sem_abreviatura_ou_nome=True
                                    )

validaMateriais.materiais(tipo_registro='materiais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                          corrigirErros=True,
                          materiais_descricao_duplicada=True
                          )

validaLotesMateriais.lotesMateriais(tipo_registro='lotes-materiais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                    corrigirErros=True,
                                    data_fabricacao_maior_que_data_de_validade=True
                                    )

validaFornecedor.fornecedores(tipo_registro='fornecedores', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              cnpj_cpf_invalido=True,
                              cnpj_cpf_nulo=True,
                              fornecedor_estado_nulo=True,
                              cnpj_cpf_duplicado=True,
                              inscricao_estadual_duplicada=True,
                              inscricao_municipal_duplicada=True,
                              data_situacao_maior_data_atual=True)

validaResponsaveis.responsaveis(tipo_registro='responsaveis', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                responsavel_cpf_nulo_ou_invalido=True)

validaOrganograma.organograma(tipo_registro='organogramas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              mascara_incorreta_nivel_zerado=True)

validaEntradasItens.entradasItens(tipo_registro='entradas-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                  corrigirErros=True,
                                  codigo_material_duplicado=True)

validaSaidasItens.saidasItens(tipo_registro='saidas-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              codigo_material_duplicado=True,
                              saida_menor_que_a_soma_das_entradas=True)

# ------------Finaliza Pré-validações----------
timeEnd = datetime.datetime.now()
print(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Fim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
