//! The active-term and commit boundary around visible share coordinator methods.

use {
    crate::meta::{edit_native_body, function_signature, is_plain_async, parameter_ident},
    moxy::{
        ast::{FnParam, ParseError, Signature},
        token::{Ident, Span, Spanner, TokenStream},
    },
    proc_macro::{TokenStream as NativeTokenStream, TokenTree},
};

struct Boundaries {
    admission: TokenStream,
    answer: TokenStream,
}

fn boundaries(meta: &TokenStream, signature: &Signature) -> Result<Boundaries, ParseError> {
    let active: Ident = moxy::parse!({ meta })?;
    let parameters = &signature.params.inner.inputs;
    if !is_plain_async(signature)
        || parameters.len() < 5
        || !matches!(parameters.first(), Some(FnParam::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_none())
    {
        return Err(ParseError::new(
            signature.span(),
            "expected an async share-state method with a borrowed self receiver and four key parameters",
        ));
    }
    parameter_ident(&parameters[1], "expected named metadata image")?;
    let group = parameter_ident(&parameters[2], "expected named group id")?;
    let topic = parameter_ident(&parameters[3], "expected named topic id")?;
    let partition = parameter_ident(&parameters[4], "expected named partition id")?;
    let state_partition = Ident::new("state_partition").with_span(Span::mixed_site());
    let result = Ident::new("result").with_span(Span::mixed_site());
    Ok(Boundaries {
        admission: moxy::template! {
            let {{ state_partition }} = self.state_partition_for({{ group }}, &{{ topic }}, {{ partition }});
            let {{ active }} = self.active({{ state_partition }}).await.map_err(ShareStateError::inactive)?;
            let {{ result }} =
        },
        answer: moxy::template! { ; self.answer({{ active }}, {{ result }}).await },
    })
}

pub(crate) fn expand(
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let signature = function_signature(&item.clone().into())?;
    let boundary = boundaries(meta, &signature)?;
    Ok(edit_native_body(item, |body| {
        let mut operations: NativeTokenStream = boundary.admission.into();
        let mut group = proc_macro::Group::new(proc_macro::Delimiter::Brace, body);
        group.set_span(proc_macro::Span::call_site());
        operations.extend([TokenTree::Group(group)]);
        operations.extend(NativeTokenStream::from(boundary.answer));
        operations
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::boundaries,
        crate::meta::{compact, function_signature},
        assert2::assert,
    };

    #[test]
    fn admission_precedes_the_native_operation_and_answer_follows_it() {
        let signature = function_signature(&"pub(crate) async fn read(&self, image: &Image, group: &str, topic_id: Uuid, partition: i32) -> Result<Value, Error> {}".parse().unwrap()).unwrap();
        let boundary = boundaries(&"active".parse().unwrap(), &signature).unwrap();
        assert!(
            compact(&boundary.admission)
                .contains("self.active(state_partition).await.map_err(ShareStateError::inactive)?")
        );
        assert!(compact(&boundary.answer).contains("self.answer(active,result).await"));
        assert!(boundaries(&"active extra".parse().unwrap(), &signature).is_err());
        for source in [
            "fn read(&self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async fn read(self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async fn read(&self, image: &Image) {}",
            "async unsafe fn read(&self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async extern \"C\" fn read(&self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async fn read<T>(&self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async fn read(&mut self, image: &Image, group: &str, topic: Uuid, partition: i32) {}",
            "async fn read(&self, _: &Image, group: &str, topic: Uuid, partition: i32) {}",
        ] {
            let signature = function_signature(&source.parse().unwrap()).unwrap();
            assert!(boundaries(&"active".parse().unwrap(), &signature).is_err());
        }
    }

    #[test]
    fn uses_caller_key_and_guard_names_without_exposing_private_result_bindings() {
        let signature = function_signature(&"async fn erase(&self, view: &Image, group_key: &str, topic_key: Uuid, row: i32, extra: i16) -> Result<(), Error> {}".parse().unwrap()).unwrap();
        let boundary = boundaries(&"term_guard".parse().unwrap(), &signature).unwrap();
        let admission = compact(&boundary.admission);
        assert!(admission.contains("state_partition_for(group_key,&topic_key,row)"));
        assert!(admission.contains("letterm_guard="));
        assert!(compact(&boundary.answer).contains("self.answer(term_guard,result).await"));
    }
}
