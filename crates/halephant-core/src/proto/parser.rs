use chumsky::prelude::*;

use crate::errors;

pub(in crate::proto) type PgExtra<'src> = extra::Err<Rich<'src, u8>>;

/// Run a chumsky parser on `input`, requiring that all bytes are consumed.
/// Converts chumsky errors to [`ProtocolError`].
pub(in crate::proto) fn run<'a, O>(
    parser: impl Parser<'a, &'a [u8], O, PgExtra<'a>>,
    input: &'a [u8],
) -> Result<O, errors::ProtocolError> {
    parser
        .then_ignore(end())
        .parse(input)
        .into_result()
        .map_err(|errs| {
            let err = &errs[0];
            errors::ProtocolError::InvalidValue {
                position: err.span().start,
                message: format!("{err}"),
            }
        })
}
