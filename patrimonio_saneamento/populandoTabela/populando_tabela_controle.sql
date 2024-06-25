-- descricao_unidade_maior_60_caracter
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_unidade_maior_60_caracter'
where tipo_registro = 'organograma'
and CHARINDEX('possui descrição com mais de 60 caracteres', mensagem_erro) >  0;

-- descricao_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_duplicada'
where tipo_registro = 'localizacao-fisica'
and CHARINDEX('tem duplicidade com outro cadastro."', mensagem_erro) >  0;

-- cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_duplicado'
where tipo_registro = 'responsavel'
and CHARINDEX('CPF:"', mensagem_erro) >  0
and CHARINDEX('duplicado com o responsável', mensagem_erro) >  0;

-- cod_siafe_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cod_siafe_nulo'
where tipo_registro = 'responsavel'
and CHARINDEX('possui codigo de cidade inválido', mensagem_erro) >  0;

-- cpf_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_nulo'
where tipo_registro = 'responsavel'
and CHARINDEX('Não foi informado o CPF do responsáve', mensagem_erro) >  0;

-- cpf_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cpf_invalido'
where tipo_registro = 'responsavel'
and CHARINDEX('não possui 11 dígitos!', mensagem_erro) >  0;

-- fornecedor_cidade_nula
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_cidade_nula'
where tipo_registro = 'fornecedores'
and CHARINDEX('Não foi encontrado uma cidade de inscrição para o fornecedor.', mensagem_erro) >  0;

-- cnpj_cpf_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_cpf_nulo'
where tipo_registro = 'fornecedores'
and CHARINDEX('O CPF ou CNPJ do fornecedo', mensagem_erro) >  0;

-- cnpj_cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_cpf_duplicado'
where tipo_registro = 'fornecedores'
and CHARINDEX('duplicado com o fornecedor', mensagem_erro) >  0;

-- valor_aquisicao_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'valor_aquisicao_nulo'
where tipo_registro = 'bem'
and CHARINDEX('não possui valor de aquisição informado.', mensagem_erro) >  0;

-- valor_depreciado_maior_valor_aquisicao
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'valor_depreciado_maior_valor_aquisicao'
where tipo_registro = 'bem'
and CHARINDEX('possui valor depreciado maior que o valor de aquisição.', mensagem_erro) >  0;

-- valor_depreciado_maior_valor_depreciavel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'valor_depreciado_maior_valor_depreciavel'
where tipo_registro = 'bem'
and CHARINDEX('possui valor depreciado maior que o valor depreciável.', mensagem_erro) >  0;

-- dt_depreciacao_menor_dt_aquisicao_bem
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'dt_depreciacao_menor_dt_aquisicao_bem'
where tipo_registro = 'bem'
and CHARINDEX('deve ser superior a data de aquisição do bem.', mensagem_erro) >  0;

-- bem_sem_responsavel
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'bem_sem_responsavel'
where tipo_registro = 'bem'
and CHARINDEX('não tem responsável informado!', mensagem_erro) >  0;

-- descricao_bem_maior_1024_caracter
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'descricao_bem_maior_1024_caracter'
where tipo_registro = 'bem'
and CHARINDEX('tem observação com mais de 1024 caracteres', mensagem_erro) >  0;

-- tempo_garantia_negativo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tempo_garantia_negativo'
where tipo_registro = 'bem'
and CHARINDEX('tem observação com mais de 1024 caracteres', mensagem_erro) >  0;

-- tempo_garantia_negativo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'tempo_garantia_negativo'
where tipo_registro = 'bem'
and CHARINDEX('está com o tempo de garantia negativo!', mensagem_erro) >  0;

-- vida_util_maior_zero_com_depreciacao_anual_igual_zero
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'vida_util_maior_zero_com_depreciacao_anual_igual_zero'
where tipo_registro = 'bem'
and CHARINDEX('possui a Vida útil(anos) maior que zero mas seu %', mensagem_erro) >  0;

-- valor_residual_superior_liquido_contabil
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'valor_residual_superior_liquido_contabil'
where tipo_registro = 'bem'
and CHARINDEX('possui valor residual superior ao valor líquido contábil!', mensagem_erro) >  0;

-- numero_placa_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_placa_nulo'
where tipo_registro = 'bem'
and CHARINDEX('não possui número da placa informado.', mensagem_erro) >  0;

-- placa_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'placa_duplicada'
where tipo_registro = 'bem'
and CHARINDEX('está vinculada a mais de um bem.', mensagem_erro) >  0;

-- historico_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'historico_nulo'
where tipo_registro = 'baixa'
and CHARINDEX('não tem historico informado!', mensagem_erro) >  0;

-- motivo_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'motivo_nulo'
where tipo_registro = 'baixa'
and CHARINDEX('não tem motivo informado!', mensagem_erro) >  0;

-- bem_placa_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'bem_placa_nulo'
where tipo_registro = 'baixa'
and CHARINDEX('está baixado e não possui placa informada.', mensagem_erro) >  0;

-- data_baixa_superior_data_aquisicao_bem
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_baixa_superior_data_aquisicao_bem'
where tipo_registro = 'baixa'
and CHARINDEX('possui data inferior ou igual a data de aquisição do bem', mensagem_erro) >  0;

-- numero_boletim_ocorrencia_maior_oito_caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_boletim_ocorrencia_maior_oito_caracteres'
where tipo_registro = 'baixa'
and CHARINDEX('O campo Boletim de Ocorrência da baixa', mensagem_erro) >  0;

-- numero_processo_maior_oito_caracteres
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_processo_maior_oito_caracteres'
where tipo_registro = 'baixa'
and CHARINDEX('O campo Número do Processo Administrativo da baixa', mensagem_erro) >  0;

-- numero_processo_caracter_especial
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_processo_caracter_especial'
where tipo_registro = 'baixa'
and CHARINDEX('O campo número do processo da baixa', mensagem_erro) >  0;

-- numero_boletim_ocorrencia_caracter_especial
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'numero_boletim_ocorrencia_caracter_especial'
where tipo_registro = 'baixa'
and CHARINDEX('O campo Boletim de Ocorrência da baixa', mensagem_erro) >  0;

-- codigo_bem_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'codigo_bem_nulo'
where tipo_registro = 'manutencao-bem'
and CHARINDEX('O codigo do bem é obrigatorio', mensagem_erro) >  0;

-- data_envio_manutencao_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_envio_manutencao_nulo'
where tipo_registro = 'manutencao-bem'
and CHARINDEX('A data de envio da manutenção é obrigatoria', mensagem_erro) >  0;

-- prestador_servico_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'prestador_servico_nulo'
where tipo_registro = 'manutencao-bem'
and CHARINDEX('O prestador da manutenção é obrigatorio', mensagem_erro) >  0;


commit;
