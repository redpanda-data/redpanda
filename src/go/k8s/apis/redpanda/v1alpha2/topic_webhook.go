// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha2

import (
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// log is for logging in this package.
var topiclog = logf.Log.WithName("topic-resource")

func (r *Topic) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

//+kubebuilder:webhook:path=/mutate-redpanda-com-v1alpha2-topic,mutating=true,failurePolicy=fail,sideEffects=None,groups=redpanda.com,resources=topics,verbs=create;update,versions=v1alpha2,name=mtopic.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &Topic{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *Topic) Default() {
	topiclog.Info("default", "name", r.Name)

	// TODO(user): fill in your defaulting logic.
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-redpanda-com-v1alpha2-topic,mutating=false,failurePolicy=fail,sideEffects=None,groups=redpanda.com,resources=topics,verbs=create;update,versions=v1alpha2,name=vtopic.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &Topic{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *Topic) ValidateCreate() error {
	topiclog.Info("validate create", "name", r.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *Topic) ValidateUpdate(old runtime.Object) error {
	topiclog.Info("validate update", "name", r.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *Topic) ValidateDelete() error {
	topiclog.Info("validate delete", "name", r.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil
}
