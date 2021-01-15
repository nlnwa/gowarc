/*
 * Copyright 2020 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package index

type Options struct {
	Dir string
	FileCacheSize int64
	IdCacheSize int64
	CdxCacheSize int64
}

func DefaultOptions() Options {
	return Options{
		Dir: "",
		FileCacheSize: 0,
		IdCacheSize: 0,
		CdxCacheSize: 0,
	}
}

func (opt Options) WithDir(val string) Options {
	opt.Dir = val
	return opt
}
func (opt Options) WithFileCacheSize(val int64) Options {
	opt.FileCacheSize = val
	return opt
}
func (opt Options) WithIdCacheSize(val int64) Options {
	opt.IdCacheSize = val
	return opt
}
func (opt Options) WithCdxCacheSize(val int64) Options {
	opt.CdxCacheSize = val
	return opt
}
