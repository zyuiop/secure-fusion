use aead::rand_core::Rng;
use aead::{KeyInit, KeySizeUser};
use aes_gcm::Aes128Gcm;
use aes_gcm::aes::Aes128;
use chacha20poly1305::ChaCha20Poly1305;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crypto::aws_lc_compat::CompatAwsLcHkdfSha256KeyManager;
use crypto::cipher::Cipher;
use crypto::hmac_sha256::LegacyKeyManager;
use crypto::planning::physical::compute_aad::{ComputeAadImpl, ComputeAadUdf};
use crypto::planning::physical::decrypt::{DecryptUdf, decrypt_array};
use crypto::planning::physical::encrypt::encrypt_array;
use crypto::{CipherContext, LongTermKeyManager};
use datafusion::arrow::array::{ArrayRef, AsArray, BinaryArray};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::scalar::ScalarValue;
use datafusion::sql::ResolvedTableReference;
use rand::rng;
use std::default::Default;
use std::sync::Arc;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn get_cipher<T: Cipher + KeySizeUser + KeyInit + 'static>() -> LongTermKeyManager {
    let cipher = LegacyKeyManager::<T>::from_hex_key_and_principal_id(
        0,
        "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284",
    );

    Box::new(cipher)
}

#[cfg(feature = "crypto_aws_lc")]
fn get_aws_cipher(aws: &'static aws_lc_rs::aead::Algorithm) -> LongTermKeyManager {
    Box::new(
        CompatAwsLcHkdfSha256KeyManager::from_hex_key_and_principal_id(
            0,
            "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284",
            aws,
        ),
    )
}

fn benchmark_crypto_raw(c: &mut Criterion) {
    let aad = DataType::Binary;

    let ciphers = [
        #[cfg(feature = "crypto_aws_lc")]
        (
            "aes128-aws-lc",
            get_aws_cipher(&aws_lc_rs::aead::AES_128_GCM),
        ),
        #[cfg(feature = "crypto_aws_lc")]
        (
            "aes256-aws-lc",
            get_aws_cipher(&aws_lc_rs::aead::AES_256_GCM),
        ),
        #[cfg(feature = "crypto_aws_lc")]
        (
            "chacha20-aws-lc",
            get_aws_cipher(&aws_lc_rs::aead::CHACHA20_POLY1305),
        ),
        ("chacha20poly1305", get_cipher::<ChaCha20Poly1305>()),
        (
            "chacha8poly1305",
            get_cipher::<chacha20poly1305::ChaCha8Poly1305>(),
        ),
        ("aes128gcm", get_cipher::<Aes128Gcm>()),
        ("aes256gcm", get_cipher::<aes_gcm::Aes256Gcm>()),
        (
            "aes128ccm-32",
            get_cipher::<ccm::Ccm<Aes128, ccm::consts::U4, ccm::consts::U12>>(),
        ),
        (
            "aes128ccm-64",
            get_cipher::<ccm::Ccm<Aes128, ccm::consts::U8, ccm::consts::U12>>(),
        ),
        (
            "aes128ccm-96",
            get_cipher::<ccm::Ccm<Aes128, ccm::consts::U12, ccm::consts::U12>>(),
        ),
        ("deoxys-1", get_cipher::<deoxys::DeoxysI128>()),
        ("deoxys-2", get_cipher::<deoxys::DeoxysII128>()),
        ("aes128eax", get_cipher::<eax::Eax<Aes128>>()),
    ];

    // Realistic payload sizes for integer columns (4/8/12) and bigger ones (128/1024)
    // For AES ciphers the timings should be identical for 4/8/12 since a block is 16B
    let mut group = c.benchmark_group("fixed_num_elems");
    for elem_size in [4usize, 16, 64, 256, 2048].iter() {
        for num_elems in [8usize * 1024].iter() {
            group.throughput(Throughput::Bytes((*num_elems * *elem_size) as u64));

            for (name, cipher) in ciphers.iter() {
                let cipher = cipher.get_cipher(&CipherContext::TableColumn {
                    table_context: ResolvedTableReference {
                        schema: String::from("def").into(),
                        table: String::from("bench").into(),
                        catalog: String::from("bench").into(),
                    },
                    column_name: RANDOM_FIELD_NAME.to_string().into(),
                });

                group.bench_with_input(
                    BenchmarkId::new(format!("{name}[{num_elems}]"), elem_size),
                    &(*num_elems, *elem_size),
                    |b, (num_elems, elem_size)| {
                        b.iter_batched(
                            || {
                                let aad_array = static_aad(aad.clone(), *num_elems);
                                let input = get_encrypted_array(
                                    &cipher,
                                    *num_elems,
                                    *elem_size,
                                    aad_array.clone(),
                                );

                                (
                                    ColumnarValue::Scalar(
                                        ScalarValue::try_from_array(aad_array.as_ref(), 0).unwrap(),
                                    ),
                                    input,
                                )
                            },
                            |(aad_array, input)| {
                                decrypt_array(&cipher, input.as_binary(), aad_array)
                                    .expect("failed to decrypt array")
                            },
                            BatchSize::LargeInput,
                        )
                    },
                );
            }
        }
    }

    group.finish();
}

fn benchmark_crypto_fixed_udf(c: &mut Criterion) {
    let aad = DataType::Binary;
    let return_field = FieldRef::new(Field::new(RANDOM_FIELD_NAME, aad.clone(), false));

    let ciphers = [("aes128gcm", Arc::new(get_cipher::<Aes128Gcm>()))];

    let mut group = c.benchmark_group("udf");
    for elem_size in [4usize, 16, 64, 256, 2048].iter() {
        for num_elems in [8usize * 1024].iter() {
            group.throughput(Throughput::Bytes((*num_elems * *elem_size) as u64));

            for (name, cipher) in ciphers.iter() {
                let cipher_ctx = cipher.get_cipher(&CipherContext::TableColumn {
                    table_context: ResolvedTableReference {
                        schema: String::from("def").into(),
                        table: String::from("bench").into(),
                        catalog: String::from("bench").into(),
                    },
                    column_name: RANDOM_FIELD_NAME.to_string().into(),
                });

                group.bench_with_input(
                    BenchmarkId::new(format!("{name}-fixed-aad[{num_elems}]"), elem_size),
                    &(*num_elems, *elem_size),
                    |b, (num_elems, elem_size)| {
                        b.iter_batched(
                            || {
                                let gen_aad_udf = ComputeAadUdf::new(&aad);

                                let aad = gen_aad_udf
                                    .invoke_with_args(args_from_input(*num_elems, vec![]))
                                    .expect("failed to generate aad")
                                    .to_array_of_size(*num_elems)
                                    .expect("invalid aad");

                                let input =
                                    get_encrypted_array(&cipher_ctx, *num_elems, *elem_size, aad);

                                let decrypt_udf = DecryptUdf::new(
                                    return_field.clone(),
                                    CipherContext::TableColumn {
                                        table_context: ResolvedTableReference {
                                            schema: String::from("def").into(),
                                            table: String::from("bench").into(),
                                            catalog: String::from("bench").into(),
                                        },
                                        column_name: RANDOM_FIELD_NAME.to_string().into(),
                                    },
                                    cipher.clone(),
                                );

                                (
                                    num_elems,
                                    ColumnarValue::Array(input),
                                    gen_aad_udf,
                                    decrypt_udf,
                                )
                            },
                            |(num_rows, input, gen_aad, decrypt_udf)| {
                                let aad = gen_aad
                                    .invoke_with_args(args_from_input(*num_rows, vec![]))
                                    .expect("failed to generate aad");

                                decrypt_udf
                                    .invoke_with_args(args_from_input(*num_rows, vec![input, aad]))
                                    .expect("failed to decrypt");
                            },
                            BatchSize::LargeInput,
                        )
                    },
                );

                group.bench_with_input(
                    BenchmarkId::new(format!("{name}-1col-aad[{num_elems}]"), elem_size),
                    &(*num_elems, *elem_size),
                    |b, (num_elems, elem_size)| {
                        b.iter_batched(
                            || {
                                let gen_aad_udf = ComputeAadUdf::new(&aad);

                                let associated_column_arr = ColumnarValue::Array(Arc::new(
                                    gen_random_array(*num_elems, 16),
                                ));

                                let aad = gen_aad_udf
                                    .invoke_with_args(args_from_input(
                                        *num_elems,
                                        vec![associated_column_arr.clone()],
                                    ))
                                    .expect("failed to generate aad")
                                    .to_array_of_size(*num_elems)
                                    .expect("invalid aad");
                                let input =
                                    get_encrypted_array(&cipher_ctx, *num_elems, *elem_size, aad);

                                let decrypt_udf = DecryptUdf::new(
                                    return_field.clone(),
                                    CipherContext::TableColumn {
                                        table_context: ResolvedTableReference {
                                            schema: String::from("def").into(),
                                            table: String::from("bench").into(),
                                            catalog: String::from("bench").into(),
                                        },
                                        column_name: RANDOM_FIELD_NAME.to_string().into(),
                                    },
                                    cipher.clone(),
                                );

                                (
                                    num_elems,
                                    associated_column_arr,
                                    ColumnarValue::Array(input),
                                    gen_aad_udf,
                                    decrypt_udf,
                                )
                            },
                            |(num_rows, assocoated_col, input, gen_aad, decrypt_udf)| {
                                let aad = gen_aad
                                    .invoke_with_args(args_from_input(
                                        *num_rows,
                                        vec![assocoated_col],
                                    ))
                                    .expect("failed to generate aad");

                                decrypt_udf
                                    .invoke_with_args(args_from_input(*num_rows, vec![input, aad]))
                                    .expect("failed to decrypt");
                            },
                            BatchSize::LargeInput,
                        )
                    },
                );

                group.bench_with_input(
                    BenchmarkId::new(format!("{name}-2cols-aad[{num_elems}]"), elem_size),
                    &(*num_elems, *elem_size),
                    |b, (num_elems, elem_size)| {
                        b.iter_batched(
                            || {
                                let gen_aad_udf = ComputeAadUdf::new(&aad);

                                let associated_column_arr1 = ColumnarValue::Array(Arc::new(
                                    gen_random_array(*num_elems, 16),
                                ));
                                let associated_column_arr2 = ColumnarValue::Array(Arc::new(
                                    gen_random_array(*num_elems, 16),
                                ));

                                let aad = gen_aad_udf
                                    .invoke_with_args(args_from_input(
                                        *num_elems,
                                        vec![
                                            associated_column_arr1.clone(),
                                            associated_column_arr2.clone(),
                                        ],
                                    ))
                                    .expect("failed to generate aad")
                                    .to_array_of_size(*num_elems)
                                    .expect("invalid aad");
                                let input =
                                    get_encrypted_array(&cipher_ctx, *num_elems, *elem_size, aad);

                                let decrypt_udf = DecryptUdf::new(
                                    return_field.clone(),
                                    CipherContext::TableColumn {
                                        table_context: ResolvedTableReference {
                                            schema: String::from("def").into(),
                                            table: String::from("bench").into(),
                                            catalog: String::from("bench").into(),
                                        },
                                        column_name: RANDOM_FIELD_NAME.to_string().into(),
                                    },
                                    cipher.clone(),
                                );

                                (
                                    num_elems,
                                    vec![associated_column_arr1, associated_column_arr2],
                                    ColumnarValue::Array(input),
                                    gen_aad_udf,
                                    decrypt_udf,
                                )
                            },
                            |(num_rows, assocoated_col, input, gen_aad, decrypt_udf)| {
                                let aad = gen_aad
                                    .invoke_with_args(args_from_input(*num_rows, assocoated_col))
                                    .expect("failed to generate aad");

                                decrypt_udf
                                    .invoke_with_args(args_from_input(*num_rows, vec![input, aad]))
                                    .expect("failed to decrypt");
                            },
                            BatchSize::LargeInput,
                        )
                    },
                );
            }
        }
    }

    group.finish()
}

fn args_from_input(num_rows: usize, input: Vec<ColumnarValue>) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        number_rows: num_rows,
        args: input,
        // The rest does not matter
        config_options: Arc::new(ConfigOptions::default()),
        return_field: FieldRef::new(Field::new(RANDOM_FIELD_NAME, DataType::Binary, false)),
        arg_fields: vec![],
    }
}

criterion_group!(benches, benchmark_crypto_fixed_udf, benchmark_crypto_raw);
criterion_main!(benches);

const RANDOM_FIELD_NAME: &str = "rand_value";

fn gen_random_array(size: usize, random_length: usize) -> BinaryArray {
    let mut output_vec = vec![0u8; size * random_length];
    rng().fill_bytes(&mut output_vec);
    let buffer = Buffer::from_vec(output_vec);

    let array = BinaryArray::try_new(
        OffsetBuffer::from_repeated_length(random_length, size),
        buffer,
        None,
    )
    .unwrap();

    array
}

fn static_aad(aad: DataType, size: usize) -> ArrayRef {
    ComputeAadImpl::new(&aad)
        .evaluate_aad(size, vec![])
        .unwrap()
        .to_array_of_size(size)
        .unwrap()
}

fn get_encrypted_array(
    ciper: &Arc<dyn Cipher>,
    size: usize,
    random_length: usize,
    aad: ArrayRef,
) -> ArrayRef {
    let array = gen_random_array(size, random_length);
    encrypt_array(ciper, &array, aad.as_binary())
}
