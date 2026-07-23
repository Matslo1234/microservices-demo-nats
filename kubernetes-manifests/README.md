# ./kubernetes-manifests

:warning: Kubernetes manifests provided in this directory are not directly
deployable to a cluster. They are meant to be used with `skaffold` command to
insert the correct `image:` tags.

Use the manifests in [/release](/release) directory which are configured with
pre-built public images.

The standalone [observability](observability) kustomization is an exception: it
uses pinned upstream images and can be applied directly with `kubectl apply -k`.
